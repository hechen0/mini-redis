//! Rust 文件写入操作指南 —— 从基础到异步
//!
//! 本示例演示了在 Rust 中将数据写入文件的多种常用方法。
//!
//! 运行方式:
//!     cargo run --example file_ops

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write, BufWriter};

/// 1. std::fs::write - 最简单的“一行式”写法
/// 适用于一次性写入整个文件，内部会自动处理文件的打开和关闭。
fn demo_simple_write() -> io::Result<()> {
    println!("===== 1. std::fs::write =====");
    let path = "hello_simple.txt";
    fs::write(path, "hello world")?;
    println!("  已写入 {path}");
    Ok(())
}

/// 2. 使用 File::create 和 Write 辅助（write_all）
/// 这种方式手动打开文件，适用于需要更细粒度控制的场景。
fn demo_file_create() -> io::Result<()> {
    println!("\n===== 2. File::create + write_all =====");
    let path = "hello_manual.txt";
    let mut file = File::create(path)?; // 如果文件存在则覆盖
    file.write_all(b"hello world")?;    // b"" 将字符串转为字节切片
    println!("  已写入 {path}");
    Ok(())
}

/// 3. 使用 BufWriter - 高性能缓冲写入
/// 适用于频繁进行多次小规模写入的场景，可以显著减少系统调用次数。
fn demo_buffered_write() -> io::Result<()> {
    println!("\n===== 3. BufWriter (带缓冲的写入) =====");
    let path = "hello_buffered.txt";
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    
    writer.write_all(b"hello")?;
    writer.write_all(b" ")?;
    writer.write_all(b"world")?;
    
    writer.flush()?; // 确保所有缓冲区内容刷新到磁盘
    println!("  已通过缓冲区写入 {path}");
    Ok(())
}

/// 4. 使用 OpenOptions - 追加(Append)或高级配置
/// 允许你指定“追加”模式，而不是默认的“覆盖”模式。
fn demo_append() -> io::Result<()> {
    println!("\n===== 4. OpenOptions (追加模式) =====");
    let path = "hello_append.txt";
    
    // 第一次写入：创建并写入
    fs::write(path, "hello")?;
    
    // 第二次写入：追加
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)?;
    
    file.write_all(b" world (appended)")?;
    println!("  已追加内容到 {path}");
    Ok(())
}

/// 5. Tokio 异步写入
/// 在异步程序（如本 mini-redis 项目）中，应使用异步文件 IO 避免阻塞执行线程。
async fn demo_tokio_write() -> io::Result<()> {
    println!("\n===== 5. Tokio Async Write (异步写入) =====");
    use tokio::fs as tfs;
    use tokio::io::AsyncWriteExt;

    let path = "hello_tokio.txt";
    // Tokio 提供了与标准库类似的接口，但它们是异步的
    tfs::write(path, "hello world from tokio").await?;
    
    // 也可以手动流式写入
    let mut file = tfs::File::create("hello_tokio_manual.txt").await?;
    file.write_all(b"hello async world").await?;
    
    println!("  已完成异步写入");
    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("╔══════════════════════════════════════════╗");
    println!("║   Rust 文件操作演示 —— Hello World       ║");
    println!("╚══════════════════════════════════════════╝");

    demo_simple_write()?;
    demo_file_create()?;
    demo_buffered_write()?;
    demo_append()?;
    demo_tokio_write().await?;

    println!("\n✅ 全部演示完成！生成的临时文件：hello_*.txt");
    Ok(())
}
