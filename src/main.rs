#![feature(pattern)]

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use indicatif::ParallelProgressIterator;
use bzip2::read::BzDecoder;
use clap::{App, Arg};
use rayon::prelude::*;
use regex::{Regex, RegexBuilder};
use fnv::{FnvHashMap, FnvHashSet};
use std::fs::File;
use std::io::{prelude::*, BufReader, BufWriter, SeekFrom};
use std::path::Path;

fn main() {
    let args = App::new("English Wiki Frequency List Generator")
        .version("0.1.0")
        .author("Bence M. <bence.me@gmail.com>")
        .about("Generates a frequency list from an English Wikipedia dump")
        .arg(Arg::with_name("DUMP")
            .about("Sets the multistream xml bz2 dump file to use")
            .short('d')
            .long("dump"))
        .arg(Arg::with_name("INDEX")
            .about("Sets the multistream dump file index to use (defaults to xxx-multistream-index.txt.bz2)")
            .short('i')
            .long("index"))
        .arg(Arg::with_name("WIKTIONARY_INDEX")
            .about("Sets the wiktionary index file to use")
            .short('w')
            .long("windex"))
        .get_matches();

    let dump_path = args
        .value_of("DUMP")
        .map(str::to_string)
        .or_else(|| {
            find_file(
                ".",
                &Regex::new(r"enwiki-\d+-pages-articles-multistream.xml.bz2").unwrap(),
            )
        })
        .expect("no dump file specified and no file found from heuristic search");

    let index_path = args
        .value_of("INDEX")
        .map(str::to_string)
        .unwrap_or_else(|| {
            const EXT: &str = ".xml.bz2";
            if dump_path.ends_with(EXT) {
                dump_path[..dump_path.len() - EXT.len()].to_string() + "-index.txt.bz2"
            } else {
                panic!("Can't determine index file path automatically")
            }
        });

    let windex_path = args
        .value_of("WIKTIONARY_INDEX")
        .map(str::to_string)
        .or_else(|| {
            find_file(
                ".",
                &Regex::new(r"enwiktionary-\d+-pages-articles-multistream-index.txt.bz2").unwrap(),
            )
        })
        .expect("no wiktionary index file specified and no file found from heuristic search");

    println!(
        "Files being used:\n\t{}\n\t{}\n\t{}",
        dump_path, index_path, windex_path
    );

    let start = std::time::Instant::now();
    let wordset = wiktionary_index_to_wordset(windex_path);
    println!("Read wiktionary index in {:?}, found {} items", start.elapsed(), wordset.len());
    println!("A few words from the wordlist: {:?}", wordset.iter().filter(|w| w.len() <= 5).take(10).collect::<Vec<_>>());

    let start = std::time::Instant::now();
    let mut counts: Vec<_> = ArticleBlockIter::new(dump_path, index_path)
        // .take(100)
        .par_bridge()
        // .panic_fuse()
        .map(|mut block| block.process(&wordset))
        .progress()
        .reduce(FnvHashMap::default, |mut acc, e| {
            for (k, v) in e {
                *acc.entry(k).or_insert(0) += v;
            }
            acc
        })
        .into_iter()
        .collect();
    
    println!("Counting words took {:?}", start.elapsed());

    let start = std::time::Instant::now();
    counts.sort_unstable_by_key(|&(_, v)| -(v as isize));
    let mut writer =
        BufWriter::new(File::create("frequency_list.txt").expect("failed to open output file"));
    for (i, (k, v)) in counts.into_iter().enumerate() {
        writeln!(writer, "{} {}", k, v).unwrap_or_else(|e| panic!("failed to write line {}: {:?}", i, e));
    }

    println!("Sorting and saving took {:?}", start.elapsed());
    println!("All done");
}

// maybe finish this, so that the XML files have a proper header/footer
// fn dump_header_footer<I, D>(dump_path: D, index_path: I) -> (String, String)
//     where
//         D: AsRef<Path>,
//         I: AsRef<Path>,
// {
//     let dump_file = File::open(dump_path).expect("failed to open dump path");

//     let mut index_file = File::open(index_path).expect("failed to open index path");

//     // look for last linebreak
//     let mut i = 1;
//     loop {
//         let mut c = vec![0];
//         index_file.seek(SeekFrom::End(-i)).unwrap();
//         index_file.read(&mut c).unwrap();
        
//         if c[0] == b'\n' {
//             break;
//         }
//         i += 1;
//     }
//     let mut index_reader = BufReader::new(BzDecoder::new(index_file));
//     let mut last_line = String::new();
//     index_reader.read_to_string(&mut last_line).unwrap();

//     let last_article = ArticleDescriptor::from_index_line(&*last_line);

//     let mut xml_header = String::new();
//     let mut xml_footer = String::new();

//     let mut header_reader = BzDecoder::new(dump_file.try_clone().unwrap());
//     header_reader.read_to_string(&mut xml_header).unwrap();

//     let mut header_reader = BzDecoder::new(dump_file.try_clone().unwrap());
//     bz_reader.total_in() is the length of the bzip part

//     (xml_header, xml_footer)
// }

fn find_file<P: AsRef<Path>>(dir: P, re: &Regex) -> Option<String> {
    for entry in dir.as_ref().read_dir().expect("find_file read_dir failed") {
        let entry = entry.expect("entry read failed");
        let file_name_os = entry.file_name();
        let file_name_cow = file_name_os.to_string_lossy();
        let file_name = file_name_cow.as_ref();
        if re.is_match(file_name) {
            return Some(file_name.to_string());
        }
    }

    return None;
}

fn find_nth<'a, P>(s: &'a str, pat: P, n: usize) -> Option<usize>
where
    P: std::str::pattern::Pattern<'a> + Clone,
{
    if n == 0 {
        return None;
    }

    let mut i = 0;
    for _ in 0..n {
        i = if let Some(mi) = &s[i..].find(pat.clone()) {
            mi + 1 + i
        } else {
            return None;
        }
    }
    return Some(i - 1);
}

fn wiktionary_index_to_wordset<P: AsRef<Path>>(path: P) -> FnvHashSet<String> {
    BufReader::new(BzDecoder::new(
        File::open(path).expect("failed to open index file"),
    ))
    .lines()
    .map(Result::unwrap)
    .enumerate()
    .filter(|(_, s)| !s.is_empty())
    .map(|(i, line)| {
        let i2 = find_nth(&*line, ':', 2)
            .unwrap_or_else(|| panic!("can't find 2nd ':' in wiktionary index line {}", i));

        line[i2 + 1..].to_string()
    })
    .filter(|s| s.chars().all(|c| c.is_ascii_alphabetic()))
    .map(|mut s| {
        // let mut deuni = deunicode(&*s);
        s.make_ascii_lowercase();
        s
    })
    .collect()
}

struct ArticleBlockIter {
    dump: File,
    index: BufReader<BzDecoder<File>>,
    line_buf: String,
}

impl ArticleBlockIter {
    pub fn new<D, I>(dump_path: D, index_path: I) -> Self
    where
        D: AsRef<Path>,
        I: AsRef<Path>,
    {
        let dump_path = dump_path.as_ref();
        let index_path = index_path.as_ref();
        let index_file = File::open(index_path).expect("unable to open index file");

        Self {
            dump: File::open(dump_path).expect("unable to open dump file"),
            index: BufReader::new(BzDecoder::new(index_file)),
            line_buf: String::new(),
        }
    }
}

impl Iterator for ArticleBlockIter {
    type Item = DumpBlock;
    fn next(&mut self) -> Option<DumpBlock> {
        let mut descriptors = Vec::new();

        if !self.line_buf.is_empty() {
            descriptors.push(ArticleDescriptor::from_index_line(self.line_buf.trim()));
            self.line_buf.clear();
        }

        loop {
            match self.index.read_line(&mut self.line_buf) {
                Ok(0) => break,
                Ok(_) => {
                    let line = self.line_buf.trim();
                    let new_desc = ArticleDescriptor::from_index_line(line);

                    if let Some(last_desc) = descriptors.last() {
                        if new_desc.offset != last_desc.offset {
                            break;
                        }
                    }

                    descriptors.push(new_desc);

                    if descriptors.len() > 100 {
                        panic!("added more than 100 descriptors per block");
                    }
                }
                Err(e) => panic!("error reading index line: {}", e),
            }
            self.line_buf.clear();
        }

        if descriptors.is_empty() {
            return None;
        }

        self.dump
            .seek(SeekFrom::Start(descriptors[0].offset))
            .expect("dump file seek failed");

        let mut bz_reader = BzDecoder::new(self.dump.try_clone().expect("dump file clone failed"));
        let mut raw_xml = String::new();

        raw_xml.push_str(r"<dummyroot>");
        bz_reader
            .read_to_string(&mut raw_xml)
            .expect("dump file bzip decode failed");
        raw_xml.push_str(r"</dummyroot>");

        let block = DumpBlock {
            descriptors,
            raw_xml,
        };

        // println!("Built block {:?}", block);

        return Some(block);
    }
}

struct DumpBlock {
    descriptors: Vec<ArticleDescriptor>,
    raw_xml: String,
}

impl std::fmt::Debug for DumpBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "DumpBlock {{ {}-{} ({}-{}), {} descriptors, {} bytes in xml }}",
            self.descriptors[0].title,
            self.descriptors[self.descriptors.len()-1].title,
            self.descriptors[0].id,
            self.descriptors[self.descriptors.len()-1].id,
            self.descriptors.len(),
            self.raw_xml.len())
    }
}

impl DumpBlock {
    pub fn process(&mut self, wordset: &FnvHashSet<String>) -> FnvHashMap<String, usize> {
        thread_local! {
            static WORD_REGEX: Regex = RegexBuilder::new(r"\w(?:(?:\.|\-|')?\w+)*")
                .case_insensitive(true)
                .build()
                .expect("WORD_REGEX build failed");
        }

        // println!(
        //     "Reading ID range {}-{} ({} entries)",
        //     self.descriptors[0].id,
        //     self.descriptors[self.descriptors.len()-1].id,
        //     self.descriptors.len(),
        // );

        let mut counts = FnvHashMap::default();

        WORD_REGEX.with(|re| {
            let doc = roxmltree::Document::parse(&*self.raw_xml)
                .unwrap_or_else(|e| panic!("failed to parse xml in {:?}: {:?}\n{:#?}", self, e, &self.raw_xml[..1000]));

            for text_node in doc.descendants().filter(|n| n.has_tag_name("text")) {
                let text = text_node.text();
                if text.is_none() { continue; }

                for mat in re.find_iter(text.unwrap()) {
                    let word: String = mat.as_str()
                        .chars()
                        .filter(char::is_ascii_alphabetic)
                        .map(|c| c.to_ascii_lowercase())
                        .collect();
                    
                    if word.is_empty() { continue; }
                    
                    if wordset.contains(&word) {
                        *counts.entry(word).or_insert(0) += 1;
                    }
                }
            }
        });

        // println!(
        //     "Counted {} words in {} entries",
        //     counts.values().sum::<usize>(),
        //     self.descriptors.len()
        // );

        return counts;
    }
}

#[derive(Debug, Clone)]
struct ArticleDescriptor {
    offset: u64,
    id: usize,
    title: String,
}

impl ArticleDescriptor {
    pub fn from_index_line(line: &str) -> Self {
        let i1 = find_nth(line, ':', 1).expect("offset read failed");
        let i2 = find_nth(line, ':', 2).expect("id read failed");

        Self {
            offset: line[..i1].parse().expect("offset parse failed"),
            id: line[i1 + 1..i2].parse().expect("id parse failed"),
            title: line[i2 + 1..].to_string(),
        }
    }
}
