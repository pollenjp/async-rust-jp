use anyhow::{bail, Context as _, Error, Result};
use async_native_tls::TlsStream;
use async_task::{Runnable, Task};
use bytes::Bytes;
use flume::{Receiver, Sender};
use futures_lite::future;
use http::Uri;
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::tokio::TokioIo;
use smol::{io, prelude::*, Async};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::pin::Pin;
use std::sync::LazyLock;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{future::Future, panic::catch_unwind, thread};

#[derive(Debug, Clone, Copy)]
enum FutureType {
    High,
    Low,
}

// Multiple Threads , Multiple Queues, with Task Stealing, refactored, with join macro, background task

fn spawn_task<F, T>(future: F, order: FutureType) -> Task<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    static HIGH_CHANNEL: LazyLock<(Sender<Runnable>, Receiver<Runnable>)> =
        LazyLock::new(flume::unbounded::<Runnable>);
    static LOW_CHANNEL: LazyLock<(Sender<Runnable>, Receiver<Runnable>)> =
        LazyLock::new(flume::unbounded::<Runnable>);

    static HIGH_QUEUE: LazyLock<flume::Sender<Runnable>> = LazyLock::new(|| {
        let high_num = std::env::var("HIGH_NUM").unwrap().parse::<usize>().unwrap();
        for _ in 0..high_num {
            let high_receiver = HIGH_CHANNEL.1.clone();
            let low_receiver = LOW_CHANNEL.1.clone();
            thread::spawn(move || loop {
                match high_receiver.try_recv() {
                    Ok(runnable) => {
                        let _ = catch_unwind(|| runnable.run());
                    }
                    Err(_) => match low_receiver.try_recv() {
                        Ok(runnable) => {
                            let _ = catch_unwind(|| runnable.run());
                        }
                        Err(_) => {
                            thread::sleep(Duration::from_millis(100));
                        }
                    },
                };
            });
        }
        HIGH_CHANNEL.0.clone()
    });
    static LOW_QUEUE: LazyLock<flume::Sender<Runnable>> = LazyLock::new(|| {
        let low_num = std::env::var("LOW_NUM").unwrap().parse::<usize>().unwrap();
        for _ in 0..low_num {
            let high_receiver = HIGH_CHANNEL.1.clone();
            let low_receiver = LOW_CHANNEL.1.clone();
            thread::spawn(move || loop {
                match low_receiver.try_recv() {
                    Ok(runnable) => {
                        let _ = catch_unwind(|| runnable.run());
                    }
                    Err(_) => match high_receiver.try_recv() {
                        Ok(runnable) => {
                            let _ = catch_unwind(|| runnable.run());
                        }
                        Err(_) => {
                            thread::sleep(Duration::from_millis(100));
                        }
                    },
                };
            });
        }
        LOW_CHANNEL.0.clone()
    });

    let schedule_high = |runnable| HIGH_QUEUE.send(runnable).unwrap();
    let schedule_low = |runnable| LOW_QUEUE.send(runnable).unwrap();

    let schedule = match order {
        FutureType::High => schedule_high,
        FutureType::Low => schedule_low,
    };
    let (runnable, task) = async_task::spawn(future, schedule);
    runnable.schedule();
    task
}

macro_rules! spawn_task {
    ($future:expr) => {
        spawn_task!($future, FutureType::Low)
    };
    ($future:expr, $order:expr) => {
        spawn_task($future, $order)
    };
}

macro_rules! join {
    ($($future:expr),*) => {
        {
            vec![
                $(
                    future::block_on($future),
                )*
            ]
        }
    };
}

#[allow(unused_macros)]
macro_rules! try_join {
    ($($future:expr),*) => {
        {
            vec![
                $(
                    catch_unwind(|| future::block_on($future)),
                )*
            ]
        }
    };
}

struct Runtime {
    high_num: usize,
    low_num: usize,
}

impl Runtime {
    pub fn new() -> Self {
        let num_cores = std::thread::available_parallelism().unwrap().get();
        Self {
            high_num: num_cores - 2,
            low_num: 1,
        }
    }
    pub fn with_high_num(mut self, num: usize) -> Self {
        self.high_num = num;
        self
    }
    pub fn with_low_num(mut self, num: usize) -> Self {
        self.low_num = num;
        self
    }
    pub fn run(&self) {
        unsafe {
            std::env::set_var("HIGH_NUM", self.high_num.to_string());
            std::env::set_var("LOW_NUM", self.low_num.to_string());
        }
        let high = spawn_task!(async {}, FutureType::High);
        let low = spawn_task!(async {}, FutureType::Low);
        join!(high, low);
    }
}

#[derive(Debug, Clone, Copy)]
struct BackgroundProcess;

impl Future for BackgroundProcess {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("background process firing");
        std::thread::sleep(Duration::from_secs(1));
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

enum CustomStream {
    Plain(Async<TcpStream>),
    Tls(TlsStream<Async<TcpStream>>),
}

#[derive(Clone)]
struct CustomConnector;

impl hyper::service::Service<Uri> for CustomConnector {
    type Response = CustomStream;
    type Error = Error;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;
    fn call(&self, uri: Uri) -> Self::Future {
        Box::pin(async move { uri_to_stream(uri).await })
    }
}

async fn uri_to_stream(uri: Uri) -> std::result::Result<CustomStream, Error> {
    let host = uri.host().context("cannot parse host")?;
    match uri.scheme_str() {
        Some("http") => {
            let socket_addr = {
                let host = host.to_string();
                let port = uri.port_u16().unwrap_or(80);
                smol::unblock(move || (host, port).to_socket_addrs())
                    .await?
                    .next()
                    .context("cannot resolve address")?
            };
            let stream = Async::<TcpStream>::connect(socket_addr).await?;
            Ok(CustomStream::Plain(stream))
        }
        Some("https") => {
            let socket_addr = {
                let host = host.to_string();
                let port = uri.port_u16().unwrap_or(443);
                smol::unblock(move || (host, port).to_socket_addrs())
                    .await?
                    .next()
                    .context("cannot resolve address")?
            };
            let stream = Async::<TcpStream>::connect(socket_addr).await?;
            let stream = async_native_tls::connect(host, stream).await?;
            Ok(CustomStream::Tls(stream))
        }
        scheme => bail!("unsupported scheme: {:?}", scheme),
    }
}

impl tokio::io::AsyncRead for CustomStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            CustomStream::Plain(s) => {
                Pin::new(s)
                    .poll_read(cx, buf.initialize_unfilled())
                    .map_ok(|size| {
                        buf.advance(size);
                    })
            }
            CustomStream::Tls(s) => {
                Pin::new(s)
                    .poll_read(cx, buf.initialize_unfilled())
                    .map_ok(|size| {
                        buf.advance(size);
                    })
            }
        }
    }
}

impl tokio::io::AsyncWrite for CustomStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            CustomStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            CustomStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            CustomStream::Plain(s) => Pin::new(s).poll_flush(cx),
            CustomStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            CustomStream::Plain(s) => {
                s.get_ref().shutdown(Shutdown::Write)?;
                Poll::Ready(Ok(()))
            }
            CustomStream::Tls(s) => Pin::new(s).poll_close(cx),
        }
    }
}

fn main() {
    Runtime::new().with_low_num(2).with_high_num(4).run();
    spawn_task!(BackgroundProcess {}).detach();

    // let url = "https://www.rust-lang.org";
    let url = "http://example.com";
    // let url = "https://example.com";
    let uri: Uri = url.parse().unwrap();

    let req = hyper::Request::builder()
        .method("GET")
        .uri(uri.clone())
        .header("User-Agent", "hyper/1.17")
        .header("Accept", "text/html")
        .body(Empty::<Bytes>::new())
        .unwrap();

    // https://hyper.rs/guides/1/init/runtime/
    // https://docs.rs/hyper/latest/hyper/rt/trait.Executor.html
    // https://docs.rs/hyper/1.7.0/hyper/rt/trait.Executor.html
    let future = {
        async move || -> Result<(), Error> {
            println!("creating stream");
            let stream = uri_to_stream(uri).await?;
            let io = TokioIo::new(stream);
            println!("creating sender");
            let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
            println!("connecting");
            spawn_task!(async {
                println!("waiting for connection");
                if let Err(err) = conn.await {
                    println!("Connection failed: {:?}", err);
                }
            })
            .detach();
            println!("sending request");
            let mut res = sender.send_request(req).await?;
            println!("received response");

            println!("Response status: {}", res.status());
            println!("Response headers: {:?}", res.headers());

            println!("==============================================");
            // Stream the body, writing each chunk to stdout as we get it
            // (instead of buffering and printing at the end).
            while let Some(next) = res.frame().await {
                let frame = next.unwrap();
                if let Some(chunk) = frame.data_ref() {
                    print!("{}", String::from_utf8_lossy(chunk));
                }
            }
            Ok(())
        }
    }();

    println!("spawning task");
    future::block_on(spawn_task!(future)).expect("failed to send request");
}
