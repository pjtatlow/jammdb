use jammdb::Error;
use page_size::get as get_page_size;

mod common;

use common::record::*;

#[test]
fn huge_values() -> Result<(), Error> {
    TestDetails {
        name: "huge_values",
        page_size: get_page_size(),
        iterations: 100,
        inserts: SizeParams { min: 1, max: 20 },
        updates: SizeParams { min: 0, max: 5 },
        deletes: SizeParams { min: 0, max: 20 },
        num_buckets: SizeParams { min: 1, max: 3 },
        key_size: SizeParams { min: 20, max: 100 },
        value_size: SizeParams {
            min: get_page_size(),
            max: get_page_size() * 4,
        },
        buckets: vec![vec!["data1"]],
    }
    .run()
}

#[test]
fn pagesize_values() -> Result<(), Error> {
    TestDetails {
        name: "pagesize_values",
        page_size: get_page_size(),
        iterations: 100,
        inserts: SizeParams { min: 1, max: 20 },
        updates: SizeParams { min: 0, max: 5 },
        deletes: SizeParams { min: 0, max: 20 },
        num_buckets: SizeParams { min: 1, max: 3 },
        key_size: SizeParams { min: 20, max: 100 },
        value_size: SizeParams {
            min: 512,
            max: get_page_size(),
        },
        buckets: vec![vec!["data1"]],
    }
    .run()
}

#[test]
fn tiny_values() -> Result<(), Error> {
    TestDetails {
        name: "tiny_values",
        page_size: get_page_size(),
        iterations: 100,
        inserts: SizeParams { min: 1, max: 20 },
        updates: SizeParams { min: 0, max: 5 },
        deletes: SizeParams { min: 0, max: 20 },
        num_buckets: SizeParams { min: 1, max: 3 },
        key_size: SizeParams { min: 20, max: 100 },
        value_size: SizeParams { min: 32, max: 64 },
        buckets: vec![vec!["data1"]],
    }
    .run()
}

#[test]
fn lots_of_nested_buckets() -> Result<(), Error> {
    TestDetails {
        name: "lots_of_nested_buckets",
        page_size: get_page_size(),
        iterations: 100,
        inserts: SizeParams { min: 1, max: 20 },
        updates: SizeParams { min: 0, max: 5 },
        deletes: SizeParams { min: 0, max: 20 },
        num_buckets: SizeParams { min: 15, max: 30 },
        key_size: SizeParams { min: 20, max: 100 },
        value_size: SizeParams { min: 32, max: 64 },
        buckets: vec![
            vec!["data1"],
            vec!["data1", "nested1"],
            vec!["data1", "nested1", "double-nested1"],
            vec!["data1", "nested1", "double-nested2"],
            vec!["data1", "nested1", "double-nested3"],
            vec!["data1", "nested2"],
            vec!["data1", "nested2", "double-nested1"],
            vec!["data1", "nested2", "double-nested2"],
            vec!["data1", "nested2", "double-nested3"],
            vec!["data1", "nested3"],
            vec!["data1", "nested3", "double-nested1"],
            vec!["data1", "nested3", "double-nested2"],
            vec!["data1", "nested3", "double-nested3"],
            vec!["data2"],
            vec!["data2", "nested1"],
            vec!["data2", "nested1", "double-nested1"],
            vec!["data2", "nested1", "double-nested2"],
            vec!["data2", "nested1", "double-nested3"],
            vec!["data2", "nested2"],
            vec!["data2", "nested2", "double-nested1"],
            vec!["data2", "nested2", "double-nested2"],
            vec!["data2", "nested2", "double-nested3"],
            vec!["data2", "nested3"],
            vec!["data2", "nested3", "double-nested1"],
            vec!["data2", "nested3", "double-nested2"],
            vec!["data2", "nested3", "double-nested3"],
            vec!["data3"],
            vec!["data3", "nested1"],
            vec!["data3", "nested1", "double-nested1"],
            vec!["data3", "nested1", "double-nested2"],
            vec!["data3", "nested1", "double-nested3"],
            vec!["data3", "nested2"],
            vec!["data3", "nested2", "double-nested1"],
            vec!["data3", "nested2", "double-nested2"],
            vec!["data3", "nested2", "double-nested3"],
            vec!["data3", "nested3"],
            vec!["data3", "nested3", "double-nested1"],
            vec!["data3", "nested3", "double-nested2"],
            vec!["data3", "nested3", "double-nested3"],
        ],
    }
    .run()
}

#[test]
fn failure_1() -> Result<(), Error> {
    log_playback("tests/recordings/failure1.log")
}

#[test]
fn failure_2() -> Result<(), Error> {
    log_playback("tests/recordings/failure2.log")
}

#[test]
fn failure_3() -> Result<(), Error> {
    log_playback("tests/recordings/failure3.log")
}

#[test]
fn failure_4() -> Result<(), Error> {
    log_playback("tests/recordings/failure4.log")
}

#[test]
fn failure_5() -> Result<(), Error> {
    log_playback("tests/recordings/failure5.log")
}
