use std::io::Error;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn read_cluster_metadata() -> Result<(), Error>{
    let mut file = File::open("tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log").await?;
    // let mut contents = vec![];
    // file.read_to_end(&mut contents);

    let mut record_batch = Vec::new();
    let mut offset_buf = [0_u8; 8];
    let mut len_buf = [0_u8; 4];

    loop {
        let data_offset = file.read(&mut offset_buf).await?;
        let data_len = file.read(&mut len_buf).await?;
        if data_offset == 0 || data_len == 0 {
            break;
        }
        
        let mut msg_buf = vec![0_u8; i32::from_be_bytes(len_buf) as usize];
        file.read_exact(&mut msg_buf).await?;
        record_batch.push(msg_buf);

    }

    println!("Record Batch Len - {}", record_batch.len());
    println!("File Record Batch - {:?}", record_batch);
    Ok(())
}