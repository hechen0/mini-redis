//! Rust 并发编程概念 —— 由浅入深
//!
//! 本示例通过一系列独立函数，逐步演示 Rust 并发编程的核心概念。
//!
//! 运行方式:
//!     cargo run --example concurrency

#![warn(rust_2018_idioms)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot, Barrier, Mutex, RwLock, Semaphore};

// ─────────────────────────────────────────────
// 1. spawn: 创建异步任务
// ─────────────────────────────────────────────
//
// tokio::spawn 将一个 future 提交到运行时，使其并发执行。
// 它返回一个 JoinHandle，可以用 .await 拿到任务返回值。
async fn demo_spawn() {
    println!("\n===== 1. tokio::spawn: 创建异步任务 =====");

    // spawn 返回 JoinHandle<T>，把 future 提交给运行时并发执行
    let handle = tokio::spawn(async {
        println!("  [spawn] 子任务开始执行");
        42
    });

    // .await 等待子任务完成并拿到返回值
    let result = handle.await.unwrap();
    println!("  [spawn] 子任务返回: {result}");
}

// ─────────────────────────────────────────────
// 2. JoinSet: 管理多个并发任务
// ─────────────────────────────────────────────
//
// JoinSet 可以批量 spawn 多个任务，然后按完成顺序收集结果。
// 比手动收集多个 JoinHandle 更方便。
async fn demo_joinset() {
    println!("\n===== 2. JoinSet: 批量管理并发任务 =====");

    let mut set = tokio::task::JoinSet::new();

    for i in 0..5 {
        // 每个任务模拟不同耗时
        set.spawn(async move {
            tokio::time::sleep(Duration::from_millis(50 * (5 - i))).await;
            i
        });
    }

    // join_next 按任务完成顺序取出结果（不保证 spawn 顺序）
    while let Some(res) = set.join_next().await {
        println!("  [joinset] 任务完成，返回: {:?}", res.unwrap());
    }
}

// ─────────────────────────────────────────────
// 3. mpsc channel: 多生产者单消费者
// ─────────────────────────────────────────────
//
// mpsc = Multi-Producer, Single-Consumer
// 最常用的 channel 模式。多个发送端，一个接收端。
// 适合 "多个任务向一个地方汇报" 的场景。
async fn demo_mpsc() {
    println!("\n===== 3. mpsc channel: 多生产者单消费者 =====");

    // 创建容量为 32 的 channel
    let (tx, mut rx) = mpsc::channel::<String>(32);

    // 克隆发送端，模拟多个生产者
    for i in 0..3 {
        let tx = tx.clone();
        tokio::spawn(async move {
            tx.send(format!("来自生产者 {i} 的消息")).await.unwrap();
            println!("  [mpsc] 生产者 {i} 已发送");
        });
    }

    // 必须 drop 原始 tx，否则 rx.recv() 会永远等待
    drop(tx);

    // 接收所有消息
    while let Some(msg) = rx.recv().await {
        println!("  [mpsc] 消费者收到: {msg}");
    }
}

// ─────────────────────────────────────────────
// 4. oneshot channel: 一次性通信
// ─────────────────────────────────────────────
//
// oneshot 顾名思义，只能发送一次。常用于 "请求-响应" 模式。
// 发送一个任务，然后通过 oneshot 把结果返回。
async fn demo_oneshot() {
    println!("\n===== 4. oneshot channel: 一次性请求-响应 =====");

    let (tx, rx) = oneshot::channel::<String>();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        // 只能发送一次，第二次 send 会编译错误（tx 被 move 后就没了）
        tx.send("计算完成: 3.14159".to_string()).unwrap();
    });

    // .await 等待那一次发送
    let result = rx.await.unwrap();
    println!("  [oneshot] 收到响应: {result}");
}

// ─────────────────────────────────────────────
// 5. broadcast channel: 广播
// ─────────────────────────────────────────────
//
// 一个发送者，多个接收者都能收到同样的消息。
// 典型场景：事件通知、日志广播。
async fn demo_broadcast() {
    println!("\n===== 5. broadcast channel: 一发多收 =====");

    let (tx, _) = broadcast::channel::<String>(16);

    // 每个订阅者需要自己的接收端
    let mut rx1 = tx.subscribe();
    let mut rx2 = tx.subscribe();

    tokio::spawn(async move {
        if let Ok(msg) = rx1.recv().await {
            println!("  [broadcast] 接收者1: {msg}");
        }
    });

    tokio::spawn(async move {
        if let Ok(msg) = rx2.recv().await {
            println!("  [broadcast] 接收者2: {msg}");
        }
    });

    tx.send("大家好！这是一条广播".to_string()).unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
}

// ─────────────────────────────────────────────
// 6. Mutex: 异步互斥锁
// ─────────────────────────────────────────────
//
// tokio::sync::Mutex 和 std::sync::Mutex 的区别：
// - tokio Mutex 的 .lock() 返回 Future，等待时不阻塞线程
// - 适合在持有锁的时间里需要 .await 的场景
// - 如果不需要在持锁期间 .await，优先用 std::sync::Mutex
async fn demo_mutex() {
    println!("\n===== 6. Mutex: 异步互斥锁 =====");

    let data = Arc::new(Mutex::new(0));

    let mut handles = vec![];
    for _ in 0..10 {
        let data = Arc::clone(&data);
        handles.push(tokio::spawn(async move {
            let mut lock = data.lock().await;
            *lock += 1;
            // 锁在作用域结束时自动释放
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    println!("  [mutex] 10 个任务各加 1，最终值: {}", *data.lock().await);
}

// ─────────────────────────────────────────────
// 7. RwLock: 读写锁
// ─────────────────────────────────────────────
//
// 允许多个读者同时访问，但写者独占。
// 读多写少的场景下比 Mutex 性能更好。
async fn demo_rwlock() {
    println!("\n===== 7. RwLock: 读写锁 =====");

    let data = Arc::new(RwLock::new(vec![1, 2, 3]));

    // 多个读者可以同时持有 read lock
    let mut readers = vec![];
    for i in 0..3 {
        let data = Arc::clone(&data);
        readers.push(tokio::spawn(async move {
            let r = data.read().await;
            println!("  [rwlock] 读者 {i} 读取: {:?}", *r);
        }));
    }
    for r in readers {
        r.await.unwrap();
    }

    // 写者独占
    {
        let mut w = data.write().await;
        w.push(4);
        println!("  [rwlock] 写者写入: {:?}", *w);
    }
}

// ─────────────────────────────────────────────
// 8. Semaphore: 信号量 —— 并发限流
// ─────────────────────────────────────────────
//
// Semaphore 限制同时执行的任务数量。
// 典型场景：限制 API 并发请求数、数据库连接池大小。
async fn demo_semaphore() {
    println!("\n===== 8. Semaphore: 并发限流 =====");

    // 最多允许 2 个任务同时执行
    let semaphore = Arc::new(Semaphore::new(2));

    let mut handles = vec![];
    for i in 0..6 {
        let sem = Arc::clone(&semaphore);
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap(); // 获取许可
            println!("  [semaphore] 任务 {i} 开始 (持有许可)");
            tokio::time::sleep(Duration::from_millis(200)).await;
            println!("  [semaphore] 任务 {i} 结束");
            // _permit drop 时自动释放许可
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

// ─────────────────────────────────────────────
// 9. Barrier: 屏障 —— 多任务同步点
// ─────────────────────────────────────────────
//
// Barrier 让指定数量的任务都到达同一"屏障点"后才能继续。
// 适合 "所有任务完成阶段一，再一起进入阶段二" 的场景。
async fn demo_barrier() {
    println!("\n===== 9. Barrier: 多任务同步屏障 =====");

    let barrier = Arc::new(Barrier::new(3));

    let mut handles = vec![];
    for i in 0..3 {
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            println!("  [barrier] 任务 {i} 到达阶段一");
            barrier.wait().await; // 等3个任务都到这里才继续
            println!("  [barrier] 任务 {i} 进入阶段二");
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

// ─────────────────────────────────────────────
// 10. select!: 筞争多个 Future
// ─────────────────────────────────────────────
//
// tokio::select! 同时等待多个操作，哪个先完成就处理哪个。
// 类似 Go 的 select。常用于超时控制和多路复用。
async fn demo_select() {
    println!("\n===== 10. select!: 竞争多个 Future =====");

    let (tx, mut rx) = mpsc::channel::<&str>(1);

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = tx.send("迟到的消息").await;
    });

    // select! 谁先就绪就执行哪个分支
    tokio::select! {
        msg = rx.recv() => {
            println!("  [select] 收到消息: {msg:?}");
        }
        _ = tokio::time::sleep(Duration::from_millis(50)) => {
            println!("  [select] 超时！先到了");
        }
    }
}

// ─────────────────────────────────────────────
// 11. 综合实战: 并发扇出-扇入 (Fan-out / Fan-in)
// ─────────────────────────────────────────────
//
// Fan-out: 把工作分发给多个并发任务
// Fan-in: 把多个任务的结果汇总
// 这是最常见的并发模式之一。
async fn demo_fan_out_fan_in() {
    println!("\n===== 11. 综合实战: Fan-out / Fan-in =====");

    let start = Instant::now();
    let mut handles = vec![];
    for chunk in (1..=10).collect::<Vec<_>>().chunks(3) {
        let chunk = chunk.to_vec();
        handles.push(tokio::spawn(async move {
            let mut sum = 0u32;
            for n in chunk {
                // 模拟耗时计算
                tokio::time::sleep(Duration::from_millis(50)).await;
                sum += n * n; // 计算平方和
            }
            sum
        }));
    }

    // Fan-in: 汇总所有 worker 的结果
    let mut total = 0u32;
    for h in handles {
        total += h.await.unwrap();
    }

    println!(
        "  [fan-out-fan-in] 1-10 的平方和 = {total} (耗时: {:?})",
        start.elapsed()
    );
    println!("  [fan-out-fan-in] 对比串行: 10 * 50ms = 500ms，并发后大幅缩短");
}
// ╔══════════════════════════════════════════════╗
// ║  以下为 Rust 标准库原生并发示例 (无第三方依赖)  ║
// ╚══════════════════════════════════════════════╝

// ─────────────────────────────────────────────
// 12. std::thread: 原生操作系统线程
// ─────────────────────────────────────────────
//
// std::thread::spawn 创建真正的 OS 线程，不是协程/绿色线程。
// Rust 的线程非常轻量：栈默认 2MiB（可调），创建开销接近 pthread_create。
// JoinHandle::join() 阻塞等待线程结束，可获取返回值。
fn demo_std_thread() {
    println!("\n===== 12. std::thread: 原生 OS 线程 =====");

    let handle = thread::spawn(|| {
        println!("  [std-thread] 子线程 id = {:?}", thread::current().id());
        // spawn 时闭包内捕获的变量会被移动（move 语义）
        7 * 6
    });

    // join 阻塞当前线程，直到子线程结束
    let result = handle.join().unwrap();
    println!(
        "  [std-thread] 主线程 id = {:?}, 子线程返回: {result}",
        thread::current().id()
    );

    // 使用 move 闭包将变量所有权转移到子线程
    let msg = String::from("hello from main");
    let handle = thread::spawn(move || {
        // msg 的所有权已转移到此处
        format!("子线程收到: {msg}")
    });
    println!("  [std-thread] {}", handle.join().unwrap());

    // scoped thread: 不需要 Arc/move，可以直接借用父线程变量
    // 线程在作用域结束时自动 join，比 spawn 更安全
    let mut data = vec![1, 2, 3];
    thread::scope(|s| {
        // s.spawn 创建的线程会在 scope 结束时自动 join
        // 因此可以安全地借用变量
        s.spawn(|| {
            data.push(4);
            println!("  [std-thread] scoped 线程修改 data: {:?}", data);
        });
    }); // ← 这里所有 scoped 线程自动 join
    thread::scope(|s| {
        // 多个只读借用可以并发
        let data_ref = &data;
        for i in 0..2 {
            s.spawn(move || {
                println!("  [std-thread] scoped 线程 {i} 读取 data: {:?}", data_ref);
            });
        }
    });
    println!("  [std-thread] scope 结束后 data: {:?}", data);
}

// ─────────────────────────────────────────────
// 13. std::sync::mpsc: 标准库 channel
// ─────────────────────────────────────────────
//
// 标准库自带 mpsc channel，无需任何外部依赖。
// tx (Sender) 可以 clone 实现多生产者，rx (Receiver) 不可以 clone（单消费者）。
// send() 是非阻塞的，recv() 会阻塞当前线程。
fn demo_std_mpsc() {
    println!("\n===== 13. std::sync::mpsc: 标准库 channel =====");

    let (tx, rx) = std::sync::mpsc::channel::<String>();

    // 多生产者：clone Sender
    for i in 0..3 {
        let tx = tx.clone();
        thread::spawn(move || {
            tx.send(format!("生产者 {i} 的消息")).unwrap();
        });
    }

    // drop 原始 tx，这样当所有 clone 的 tx 都被 drop 后，rx 迭代结束
    drop(tx);

    // rx 实现了 Iterator，可以 for 循环接收
    for msg in rx {
        println!("  [std-mpsc] 收到: {msg}");
    }
}

// ─────────────────────────────────────────────
// 14. std::sync::Mutex: 标准库互斥锁
// ─────────────────────────────────────────────
//
// std::sync::Mutex 是操作系统级别的互斥锁（pthread_mutex / SRWLOCK）。
// 与 tokio::sync::Mutex 的关键区别：
//   - lock() 阻塞当前 OS 线程（不是 async）
//   - 绝对不能在持有锁期间 .await（会导致死锁）
//   - 对于纯 CPU 计算，比 async Mutex 更高效
fn demo_std_mutex() {
    println!("\n===== 14. std::sync::Mutex: 标准库互斥锁 =====");

    let counter = Arc::new(std::sync::Mutex::new(0usize));

    let mut handles = vec![];
    for _ in 0..10 {
        let counter = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            let mut lock = counter.lock().unwrap(); // 阻塞直到获取锁
            *lock += 1;
            // lock guard drop 时自动释放锁
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    println!(
        "  [std-mutex] 10 个线程各加 1，最终值: {}",
        *counter.lock().unwrap()
    );

    // 演示 lock 中毒（poison）
    // 如果一个线程在持有 Mutex 时 panic，Mutex 变为 "poisoned" 状态
    // 后续 lock() 返回 Err，但可以通过 into_inner() 强制取出数据
    let poison_lock = Arc::new(std::sync::Mutex::new(42));
    let poison_clone = Arc::clone(&poison_lock);
    let _ = thread::spawn(move || {
        let _guard = poison_clone.lock().unwrap();
        panic!("故意 panic！");
    })
    .join();

    // lock 返回 PoisonError，但可以 recover
    let recovered = match poison_lock.lock() {
        Ok(guard) => *guard,
        Err(poisoned) => {
            // 数据可能处于不一致状态，但仍可访问
            *poisoned.into_inner()
        }
    };
    println!("  [std-mutex] 检测到中毒并恢复，值: {recovered}");
}

// ─────────────────────────────────────────────
// 15. std::sync::RwLock: 标准库读写锁
// ─────────────────────────────────────────────
//
// 允许多个读者同时读，但写者独占。
// 与 Mutex 类似，也是 OS 级原语，阻塞线程。
fn demo_std_rwlock() {
    println!("\n===== 15. std::sync::RwLock: 标准库读写锁 =====");

    let data = Arc::new(std::sync::RwLock::new(vec![1, 2, 3]));

    // 多个读者可以同时持有 read lock
    let mut readers = vec![];
    for i in 0..3 {
        let data = Arc::clone(&data);
        readers.push(thread::spawn(move || {
            let r = data.read().unwrap();
            println!("  [std-rwlock] 读者 {i} 读取: {:?}", *r);
            // sleep 一小段时间，证明多个读者可以并发
            thread::sleep(Duration::from_millis(50));
        }));
    }
    for r in readers {
        r.join().unwrap();
    }

    // 写者独占
    {
        let mut w = data.write().unwrap();
        w.push(4);
        println!("  [std-rwlock] 写者写入: {:?}", *w);
    }
}

// ─────────────────────────────────────────────
// 16. Atomic: 无锁原子操作
// ─────────────────────────────────────────────
//
// Atomic 类型提供无锁的线程安全操作。
// 比 Mutex 轻量得多，适合简单的计数器、标志位等。
// Ordering 控制内存可见性保证（越严格性能越低）：
//   - Relaxed: 无顺序保证，最高性能
//   - Acquire/Release: 建立同步点（常用于锁的实现）
//   - SeqCst: 最强保证，所有线程看到一致的操作顺序
fn demo_atomic() {
    println!("\n===== 16. Atomic: 无锁原子操作 =====");

    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];
    for _ in 0..1000 {
        let counter = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            // fetch_add 是原子操作：读取 → 加 → 写回，不会被中断
            // Relaxed: 不需要与其他操作建立顺序关系，性能最高
            counter.fetch_add(1, Ordering::Relaxed);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    println!(
        "  [atomic] 1000 个线程 fetch_add，结果: {} (正确值: 1000)",
        counter.load(Ordering::Relaxed)
    );

    // compare_exchange (CAS): 原子地比较并交换
    // 是无锁数据结构的基石
    let value = AtomicUsize::new(5);
    // CAS: 如果当前值 == 5，则替换为 10
    match value.compare_exchange(5, 10, Ordering::SeqCst, Ordering::SeqCst) {
        Ok(old) => println!("  [atomic] CAS 成功: {old} -> 10"),
        Err(current) => println!("  [atomic] CAS 失败，当前值: {current}"),
    }
    println!("  [atomic] 当前值: {}", value.load(Ordering::Relaxed));
}

// ─────────────────────────────────────────────
// 17. Barrier: 标准库屏障
// ─────────────────────────────────────────────
//
// 标准库也有 Barrier！让 N 个线程在屏障点汇合后再一起继续。
fn demo_std_barrier() {
    println!("\n===== 17. std::sync::Barrier: 标准库屏障 =====");

    let barrier = Arc::new(std::sync::Barrier::new(3));

    let mut handles = vec![];
    for i in 0..3 {
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            println!("  [std-barrier] 线程 {i} 到达屏障");
            barrier.wait(); // 阻塞，等3个线程都到达
            println!("  [std-barrier] 线程 {i} 通过屏障");
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

// ─────────────────────────────────────────────
// 18. Condvar: 条件变量
// ─────────────────────────────────────────────
//
// Condvar 配合 Mutex 使用，允许线程等待某个条件成立。
// wait() 会原子性地释放锁并阻塞；被唤醒时重新获取锁。
// 这是经典的生产者-消费者模式的底层原语。
fn demo_condvar() {
    println!("\n===== 18. Condvar: 条件变量 =====");

    let pair = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let pair2 = Arc::clone(&pair);

    // 消费者：等待条件满足
    let consumer = thread::spawn(move || {
        let (lock, cvar) = &*pair2;
        let mut started = lock.lock().unwrap();
        // wait_while: 如果闭包返回 true 就继续等待
        // 释放锁 → 阻塞 → 被唤醒后重新获取锁 → 检查条件
        while !*started {
            started = cvar.wait(started).unwrap();
        }
        println!("  [condvar] 消费者收到通知，条件已满足!");
    });

    // 生产者：设置条件并通知
    thread::sleep(Duration::from_millis(100));
    {
        let (lock, cvar) = &*pair;
        let mut started = lock.lock().unwrap();
        *started = true;
        // notify_one 唤醒一个在 wait 的线程
        cvar.notify_one();
        println!("  [condvar] 生产者设置条件并通知");
    }

    consumer.join().unwrap();
}

// ─────────────────────────────────────────────
// 19. 综合实战: 线程池 + channel 并行计算
// ─────────────────────────────────────────────
//
// 用纯标准库实现一个简单但完整的并行 MapReduce：
//   1. 将工作分块 (Map)
//   2. 分发给线程池
//   3. 通过 channel 收集结果 (Reduce)
fn demo_thread_pool_mapreduce() {
    println!("\n===== 19. 综合实战: 线程池 + channel 并行计算 =====");

    let data: Vec<u64> = (1..=20).collect();
    let worker_count = 4;

    let start = Instant::now();

    // 把数据分块，每块交给一个独立线程处理
    let chunk_size = (data.len() + worker_count - 1) / worker_count;

    // 结果 channel: 每个 worker 算完后通过 channel 汇报
    let (result_tx, result_rx) = std::sync::mpsc::channel::<u64>();

    let mut handles = vec![];
    for (id, chunk) in data.chunks(chunk_size).enumerate() {
        let chunk = chunk.to_vec();
        let tx = result_tx.clone();
        handles.push(thread::spawn(move || {
            // Map: 每个 worker 独立计算自己分到的数据
            let sum: u64 = chunk
                .iter()
                .map(|n| {
                    thread::sleep(Duration::from_micros(500)); // 模拟计算
                    n * n
                })
                .sum();
            println!("  [mapreduce] worker {id} 完成，chunk 和: {sum}");
            tx.send(sum).unwrap(); // 通过 channel 汇报结果
        }));
    }
    drop(result_tx); // 所有 clone 的 tx 在线程结束后 drop，关闭 channel

    // Reduce: 从 channel 收集所有 worker 的结果
    let mut total = 0u64;
    for result in result_rx {
        total += result;
    }

    // 等待所有线程结束（实际上 channel 关闭时线程应该已完成）
    for h in handles {
        h.join().unwrap();
    }

    println!(
        "  [mapreduce] 1-20 平方和 = {total}，串行预期 500μs*20=10ms，实际: {:?}",
        start.elapsed()
    );
}

// ─────────────────────────────────────────────
// 入口函数
// ─────────────────────────────────────────────
#[tokio::main]
async fn main() {
    println!("╔══════════════════════════════════════════╗");
    println!("║   Rust 并发编程概念 —— 由浅入深         ║");
    println!("╚══════════════════════════════════════════╝");

    demo_spawn().await;
    demo_joinset().await;
    demo_mpsc().await;
    demo_oneshot().await;
    demo_broadcast().await;
    demo_mutex().await;
    demo_rwlock().await;
    demo_semaphore().await;
    demo_barrier().await;
    demo_select().await;
    demo_fan_out_fan_in().await;

    println!("\n╔═══════════════════════════════════════════════╗");
    println!("║  Part 2: 标准库原生并发 (std only, 无依赖)  ║");
    println!("╚═══════════════════════════════════════════════╝");

    demo_std_thread();
    demo_std_mpsc();
    demo_std_mutex();
    demo_std_rwlock();
    demo_atomic();
    demo_std_barrier();
    demo_condvar();
    demo_thread_pool_mapreduce();

    println!("\n✅ 全部演示完成！");
}
