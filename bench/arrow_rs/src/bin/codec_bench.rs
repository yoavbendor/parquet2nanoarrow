// codec_bench dir — decompression throughput of the Rust codecs arrow-rs uses (snap, lz4_flex) on
// the buffers bench writes (<name>.snappy / <name>.lz4_raw / <name>.raw), for comparison with nanom.
use std::time::Instant;
fn main() {
    let dir = std::env::args().nth(1).unwrap();
    for k in ["rand_i64", "text", "smallint"] {
        let raw = std::fs::read(format!("{dir}/{k}.raw")).unwrap();
        for c in ["snappy", "lz4_raw"] {
            let input = std::fs::read(format!("{dir}/{k}.{c}")).unwrap();
            let mut out = vec![0u8; raw.len()];
            let mut best = f64::MAX;
            for _ in 0..200 {
                let t0 = Instant::now();
                let n = if c == "snappy" {
                    snap::raw::Decoder::new().decompress(&input, &mut out).unwrap()
                } else {
                    lz4_flex::block::decompress_into(&input, &mut out).unwrap()
                };
                best = best.min(t0.elapsed().as_secs_f64() * 1e6);
                assert_eq!(n, raw.len());
            }
            assert_eq!(out, raw);
            println!("rust  {k:8} {c:7} {best:7.1} us  {:6.0} MB/s", raw.len() as f64 / best);
        }
    }
}
