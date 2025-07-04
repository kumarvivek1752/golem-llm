use std::future::Future;

pub fn get_async_runtime() -> AsyncRuntime {
    AsyncRuntime
}

pub struct AsyncRuntime;

impl AsyncRuntime {
    pub fn block_on<F>(self, f: F) -> F::Output
    where
        F: Future,
    {
        wasi_async_runtime::block_on(|_| f)
    }
}
