extern crate clap;
extern crate parquet;

use clap::{App, Arg, SubCommand};
use parquet::schema::printer;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::metadata::FileMetaData;
use std::fs::File;
use std::path::Path;

fn main() {
	let app = App::new("Parquet Viewer")
		.about("A CLI tool to inspect parquet files.")
		.version("0.1.0")
		.author("@emagers (https://github.com/emagers)")
		.subcommand(SubCommand::with_name("count")
			.about("Gets the row count of the parquet file")
			.arg(Arg::with_name("file")
					.required(true)
					.index(1),
			))
		.subcommand(SubCommand::with_name("schema")
			.about("Displays the schema of the parquet file")
			.arg(
				Arg::with_name("file")
					.required(true)
					.index(1),
			))
		.subcommand(
			SubCommand::with_name("display")
				.about("Displays a specified number of records from the file")
				.arg(
					Arg::with_name("count")
						.short("c")
						.long("count")
						.help("The count of records to display from the start of the file")
						.takes_value(true),
				)
				.arg(
					Arg::with_name("format")
						.short("f")
						.long("format")
						.help("The format to display the output in, valid values are csv and json")
						.takes_value(true)
						.default_value("json")
				)
				.arg(
					Arg::with_name("file")
						.required(true)
						.index(1),
				)
		).arg(Arg::with_name("file")
			.required(false)
			.index(1),
		);


	let matches = app.get_matches();

	if let Some(matches) = matches.subcommand_matches("count") {
		count(matches);
	} else if let Some(matches) = matches.subcommand_matches("schema") {
		schema(matches);
	} else if let Some(matches) = matches.subcommand_matches("display") {
		display(matches);
	} else if let Some(file) = matches.value_of("file") {
		meta_data(file);
	}
}

fn get_file_reader(args: &clap::ArgMatches) -> SerializedFileReader<File> {
	let file_arg = args.value_of("file").unwrap();

	return get_file_reader_from_path(file_arg);
}

fn get_file_reader_from_path(file_name: &str) -> SerializedFileReader<File> {
	let file = File::open(&Path::new(file_name)).unwrap();

	return SerializedFileReader::new(file).unwrap();
}

fn get_file_metadata(reader: &SerializedFileReader<File>) -> &FileMetaData {
	return reader.metadata().file_metadata();
}

fn meta_data(file: &str) {
	let reader = get_file_reader_from_path(file);
	let md = get_file_metadata(&reader);

	println!("MetaData\n");

	if let Some(created_by) = md.created_by() {
		println!("{}", created_by);
	}

	println!("num_columns: {}", md.schema_descr().columns().len());
	println!("num_rows: {}", md.num_rows());
	println!("num_row_groups: {}", reader.num_row_groups());
}

fn count(args: &clap::ArgMatches) {
	let reader = get_file_reader(args);
	let meta_data = get_file_metadata(&reader);

	println!("num_rows: {}", meta_data.num_rows());
}

fn schema(args: &clap::ArgMatches) {
	let reader = get_file_reader(args);
	let meta_data = get_file_metadata(&reader);
	let schema = meta_data.schema();

	let mut buf = Vec::new();
	printer::print_schema(&mut buf, &schema);

	let string_schema = String::from_utf8(buf).unwrap();

	println!("{}", string_schema);
}

fn display(args: &clap::ArgMatches) {
	let reader = get_file_reader(args);
	let meta = get_file_metadata(&reader);

	let mut count: usize = 10;

	if let Some(count_arg) = args.value_of("count") {
		count = count_arg.parse::<usize>().unwrap();
	}

	let iter = reader.get_row_iter(None).unwrap();
	let count_iter = iter.take(count);

	match args.value_of("format") {
		Some("csv") => print_csv(count_iter),
		Some("json") => print_json(count, meta.schema_descr().columns().len(), count_iter),
		_ => {}
	}
}

fn print_csv(mut iter: std::iter::Take<parquet::record::reader::RowIter>) {
	let mut get_headers = true;
	let mut col_headers = Vec::new();

	while let Some(record) = iter.next() {
		let mut col_values = Vec::new();

		for (_, (name, field)) in record.get_column_iter().enumerate() {
			if get_headers {
				col_headers.push(name.to_string());
			}

			col_values.push(field.to_string());
		}

		if get_headers {
			println!("{}", col_headers.join(","));
		}

		println!("{}", col_values.join(","));

		get_headers = false;
	}
}

fn print_json(row_count: usize, col_count: usize, iter: std::iter::Take<parquet::record::reader::RowIter>) {
	print!("[");

	for (row_index, row) in iter.enumerate() {
		print!("{{");

		for (col_index, (name, field)) in row.get_column_iter().enumerate() {
			let out = format!("\"{}\": {}", name, field);

			if col_index == col_count - 1 {
				print!("{}", out);
			} else {
				print!("{}, ", out);
			}
		}

		if row_index == row_count - 1 {
			print!("}}");
		} else {
			print!("}},");
		}
	}

	println!("]");
}