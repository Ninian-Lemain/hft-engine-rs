use hft_engine::{
    EngineBuilder, EngineParts, EngineStorage, FlushPolicy, PersistenceWorker, RiskLimits,
};
use hft_recovery::persist_snapshot_new;
use hft_types::{
    AccountId, Command, InstrumentId, NewOrder, OrderId, PriceTicks, Quantity, SequenceNumber,
    Side, TimeInForce,
};
use std::{fmt::Debug, fs::OpenOptions, path::PathBuf};

fn describe(error: impl Debug) -> String {
    format!("{error:?}")
}

fn main() -> Result<(), String> {
    let mut args = std::env::args_os().skip(1);
    let output = PathBuf::from(args.next().ok_or("usage: lifecycle NEW_OUTPUT_DIRECTORY")?);
    if args.next().is_some() {
        return Err("usage: lifecycle NEW_OUTPUT_DIRECTORY".to_owned());
    }
    let limits = RiskLimits {
        max_quantity: Quantity(10),
        max_notional: 10_000,
        max_abs_position: Quantity(1_000),
        max_open_orders: 8,
        minimum_price: PriceTicks(1),
        maximum_price: PriceTicks(1_000),
    };
    let mut storage = EngineStorage::<4, 2>::try_new().map_err(describe)?;
    let EngineParts {
        mut engine,
        mut events,
        journal,
    } = EngineBuilder::<1, 8, 2, 4, 2>::new(InstrumentId(7), &[(AccountId(1), limits)])
        .map_err(describe)?
        .build(&mut storage)
        .map_err(describe)?;
    std::fs::create_dir(&output).map_err(describe)?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.join("journal.bin"))
        .map_err(describe)?;
    let mut worker =
        PersistenceWorker::<_, 8>::new(journal, file, FlushPolicy::OnShutdown).map_err(describe)?;
    engine
        .process_command(Command::NewOrder(NewOrder {
            order_id: OrderId(1),
            account_id: AccountId(1),
            instrument_id: InstrumentId(7),
            sequence: SequenceNumber(1),
            price: PriceTicks(100),
            quantity: Quantity(2),
            side: Side::Buy,
            time_in_force: TimeInForce::Gtc,
        }))
        .map_err(describe)?;
    engine.stop_admission();
    worker.shutdown().map_err(describe)?;
    let snapshot = engine.snapshot().map_err(describe)?;
    persist_snapshot_new(&output.join("snapshot.bin"), &snapshot).map_err(describe)?;
    let mut batches = 0;
    while events.try_pop().is_some() {
        batches += 1;
    }
    println!(
        "applied_sequence={} event_batches={batches}",
        snapshot.applied_sequence()
    );
    Ok(())
}
