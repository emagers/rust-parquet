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
	let file = File::open(&Path::new(file_arg)).unwrap();

	return SerializedFileReader::new(file).unwrap();
}

fn get_file_reader_from_path(file_name: &str) -> SerializedFileReader<File> {
	let file = File::open(&Path::new(file_name)).unwrap();

	return SerializedFileReader::new(file).unwrap();
}

fn get_file_metadata(reader: &SerializedFileReader<File>) -> &FileMetaData {
	let parquet_metadata = reader.metadata();

	return parquet_metadata.file_metadata();
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

	let count: usize;

	if let Some(count_arg) = args.value_of("count") {
		count = count_arg.parse::<usize>().unwrap();

	} else {
		count = 10;
	}

	let iter = reader.get_row_iter(None).unwrap();
	let mut count_iter = iter.take(count);

	while let Some(record) = count_iter.next() {
		println!("{}", record);
	}
}