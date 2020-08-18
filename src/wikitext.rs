use itertools::Itertools;
use regex::{Regex, RegexBuilder};

pub fn wikitext_words(text: &str) -> Vec<String> {
    thread_local! {
        static WORD: Regex = RegexBuilder::new(r"\A\w(?:(?:\.|\-|')?\w+)*")
            .case_insensitive(true)
            .build()
            .expect("WORD build failed");
        static TEMPLATE: Regex = RegexBuilder::new(r"\A\{\{.+?\}\}")
            .dot_matches_new_line(true)
            .build()
            .expect("TEMPLATE build failed");
        static LINK: Regex = RegexBuilder::new(r"\A\[.+?\]")
            .dot_matches_new_line(true)
            .build()
            .expect("LINK build failed");
        static URL: Regex = Regex::new(r"\Ahttps?://(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)")
            .expect("URL build failed");
        static SKIPS: Regex = {
            let raw = [
                r"\{\{.+?\}\}",
                r"\[.+?\]",
                r"{|.*?|}",
                r"#REDIRECT",
                r"https?://(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"
            ];

            RegexBuilder::new(&*format!(r"\A(?:{})", raw.join("|")))
                .dot_matches_new_line(true)
                .case_insensitive(true)
                .build()
                .expect("SKIPS build failed")
        };
        static END_SEC: Regex = RegexBuilder::new(r"\A=+\W*(?:See Also|External Links)\W*=+")
            .case_insensitive(true)
            .build()
            .expect("END_SEC build failed");
    }
    let html = scraper::Html::parse_fragment(text);
    let no_html_text = html
        .root_element()
        .text()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .join(" ");
    let mut text = &no_html_text[..];

    let skips = &[TEMPLATE, LINK, URL];

    let mut words = Vec::new();

    'outer: while !text.is_empty() {
        for re in skips {
            let cont = re.with(|re| {
                if let Some(mat) = re.find(text) {
                    text = &text[mat.end()..];
                    return true;
                }
                return false;
            });
            if cont {
                continue 'outer;
            }
        }
        let cont = WORD.with(|re| {
            if let Some(mat) = re.find(text) {
                let word: String = mat
                    .as_str()
                    .chars()
                    .filter(char::is_ascii_alphabetic)
                    .map(|c| c.to_ascii_lowercase())
                    .collect();

                if word.is_empty() {
                    return false;
                }

                text = &text[mat.end()..];
                words.push(word);

                return true;
            }
            return false;
        });
        if cont {
            continue 'outer;
        }

        let cont = END_SEC.with(|re| {
            return re.is_match(text);
        });
        if cont {
            break 'outer;
        }

        if let Some(i) = text.char_indices().map(|(i, _)| i).nth(1) {
            text = &text[i..];
        } else {
            break;
        }
    }

    return words;
}
