// arrow_rs_bench file.parquet [reps] — best-of-N full read of a Parquet file into Arrow
// RecordBatches with arrow-rs (one batch per row group), printing {"ms": .., "rows": ..}.
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = &args[1];
    let reps: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(5);
    let mut best = f64::MAX;
    let mut rows = 0usize;
    for _ in 0..reps {
        let t0 = Instant::now();
        let file = File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let max_rg_rows = builder
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows() as usize)
            .max()
            .unwrap_or(1)
            .max(1);
        let reader = builder.with_batch_size(max_rg_rows).build().unwrap();
        rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            rows += batch.num_rows();
            std::hint::black_box(&batch);
        }
        best = best.min(t0.elapsed().as_secs_f64() * 1e3);
    }
    println!("{{\"ms\": {:.3}, \"rows\": {}}}", best, rows);
}
