use std::io::Error;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn read_cluster_metadata() -> Result<(), Error>{
    let mut file = File::open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log").await?;

    let mut contents = vec![];
    file.read_to_end(&mut contents).await?;

    println!("File Data - {:?}", contents);
    println!("len = {}", contents.len());
    Ok(())
}