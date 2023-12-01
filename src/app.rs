use crate::{
    channel::{SubmitHandle, SubmitSource},
    replication::Workload,
};

pub async fn null_session<T, U>(mut source: SubmitSource<T, U>) -> crate::Result<()>
where
    U: Default,
{
    while let Some((_, result)) = source.option_next().await {
        result.resolve(Default::default())?
    }
    Ok(())
}

pub struct NullWorkload;

#[async_trait::async_trait]
impl Workload for NullWorkload {
    async fn session(
        &mut self,
        invoke_handle: SubmitHandle<Vec<u8>, Vec<u8>>,
    ) -> crate::Result<()> {
        invoke_handle.submit(Default::default()).await?;
        Ok(())
    }
}
