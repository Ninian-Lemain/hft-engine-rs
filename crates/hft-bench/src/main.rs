//! Benchmark binary. Prints one JSON record per cell.

use hft_bench::{ALLOCATIONS, DEALLOCATIONS};
use std::alloc::{GlobalAlloc, Layout, System};
use std::process::ExitCode;
use std::sync::atomic::Ordering;

struct CountingAllocator;

// SAFETY: every operation delegates to `System` with the identical pointer and
// layout contract. Counters do not affect allocation ownership or alignment.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: the caller provides GlobalAlloc's valid layout contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        DEALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: the caller returns the pointer with its original layout.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: the caller provides the allocation and new-size contracts.
        let resized = unsafe { System.realloc(pointer, layout, size) };
        if !resized.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            DEALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        resized
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let config = match (args.next(), args.next()) {
        (None, None) => hft_bench::SuiteConfig::full(),
        (Some(option), None) if option == "--reduced" => hft_bench::SuiteConfig::reduced(),
        _ => {
            eprintln!("usage: hft-bench [--reduced]");
            return ExitCode::from(2);
        }
    };

    // Large fixed-capacity fixtures need more stack than the Windows default.
    let handle = match std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || hft_bench::run_suite(config))
    {
        Ok(handle) => handle,
        Err(error) => {
            eprintln!("cannot start suite worker: {error}");
            return ExitCode::FAILURE;
        }
    };
    let Ok(records) = handle.join() else {
        eprintln!("suite worker panicked");
        return ExitCode::FAILURE;
    };
    for line in records {
        println!("{line}");
    }
    ExitCode::SUCCESS
}
