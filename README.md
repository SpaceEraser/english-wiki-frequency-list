# English Wikipedia Frequency List

The list is obtained by first getting a wordlist from a Wiktionary dump, then scanning the body of each article in a Wikipedia article dump. Since I couldn't find an up-to-date Wikitext parser in Rust, the program obtains frequencies by considering far more words than it should, including URLs and template code.

The current list is obtained from the 2020-08-01 dumps of Wikipedia and Wiktionary.