//
// © 2026 PLOMID Technologies INDIA
//
// PLOMID — Platform for Modern Intelligence and Data
//
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0;
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Moved from src/scalar.rs inline #[cfg(test)].
mod like_and_regex_tests {

    use plomid_executor::scalar::{cached_build_regex, like_match_regex, regex_cache_len};

    fn like(input: &str, pattern: &str, escape: Option<char>) -> bool {
        like_match_regex(input, pattern, escape, false).expect("LIKE translation is infallible")
    }

    fn ilike(input: &str, pattern: &str, escape: Option<char>) -> bool {
        like_match_regex(input, pattern, escape, true).expect("ILIKE translation is infallible")
    }

    #[test]
    fn like_semantics_match_legacy_matcher() {
        // Each row: (input, pattern, escape, expected). Mirrors the previous
        // recursive matcher's semantics, including its corner cases.
        let cases = [
            ("abc", "abc", None, true),
            ("abc", "abd", None, false),
            ("", "", None, true),
            ("a", "", None, false),
            ("", "%", None, true),
            ("abc", "%", None, true),
            ("abc", "a%c", None, true),
            ("abc", "a%d", None, false),
            ("abc", "%b%", None, true),
            ("abc", "_b_", None, true),
            ("ac", "_b_", None, false),
            ("abc", "___", None, true),
            ("ab", "___", None, false),
            ("ab", "a%%b", None, true),
            ("aXc", "a.c", None, false),
            ("a.c", "a.c", None, true),
            ("a.c", "a.%", None, true),
            ("aXc", "a.%", None, false),
            ("ABC", "abc", None, false),
            ("héllo", "h_llo", None, true),
            ("a\nb", "a%b", None, true),
            ("a\nb", "a_b", None, true),
            ("a\nb", "ab", None, false),
            // ESCAPE handling.
            ("a%b", "a\\%b", Some('\\'), true),
            ("axb", "a\\%b", Some('\\'), false),
            ("acb", "a\\%b", Some('\\'), false),
            ("a_c", "a\\_c", Some('\\'), true),
            ("abc", "a\\_c", Some('\\'), false),
            ("a\\b", "a\\\\b", Some('\\'), true),
            // No escape clause: backslash is an ordinary character.
            ("a\\%b", "a\\%b", None, true),
            // Trailing lone escape never matched under the old matcher.
            ("\\", "\\", Some('\\'), false),
            ("abc", "abc\\", Some('\\'), false),
            ("", "\\", Some('\\'), false),
            // The escape character itself may be a wildcard char.
            ("%", "%", Some('%'), false),
            ("a%b", "a%%b", Some('%'), true),
        ];
        for (input, pattern, escape, expected) in cases {
            assert_eq!(
                like(input, pattern, escape),
                expected,
                "input={input:?} pattern={pattern:?} escape={escape:?}"
            );
        }
    }

    #[test]
    fn ilike_folds_case_on_both_sides() {
        // Each row: (input, pattern, escape, expected). ILIKE applies
        // Unicode simple lowercase to input and pattern before the wildcard
        // match, so `%`/`_` keep their positions and ESCAPE still works.
        let cases = [
            ("ABC", "abc", None, true),
            ("abc", "ABC", None, true),
            ("AbC", "a%c", None, true),
            ("ABC", "a_c", None, true),
            ("ABC", "xyz", None, false),
            ("ABC", "abc", None, true),
            // Wildcards still wildcard after folding.
            ("aXYZb", "A%B", None, true),
            ("abc", "A_C", None, true),
            // ESCAPE is honored on the folded pattern.
            ("a%b", "A\\%B", Some('\\'), true),
            ("axb", "A\\%B", Some('\\'), false),
            ("a_c", "A\\_C", Some('\\'), true),
            // Unicode folding beyond ASCII.
            ("ÄPFEL", "äpfel", None, true),
            ("STRASSE", "strasse", None, true),
            // Empty / wildcard-only patterns.
            ("", "%", None, true),
            ("ABC", "%", None, true),
            ("", "", None, true),
        ];
        for (input, pattern, escape, expected) in cases {
            assert_eq!(
                ilike(input, pattern, escape),
                expected,
                "input={input:?} pattern={pattern:?} escape={escape:?}"
            );
        }
        // LIKE stays case-sensitive through the same entry point.
        assert!(!like("ABC", "abc", None));
        assert!(like("ABC", "ABC", None));
    }

    #[test]
    fn like_multi_percent_is_linear_not_exponential() {
        // 25 `%`-separated segments: the old backtracker explored ~2^25
        // splits on a non-matching input (effectively a hang). The
        // regex-backed matcher answers instantly either way.
        let pattern = format!("{}b", "a%".repeat(25));
        let non_matching = "a".repeat(5000) + "c";
        assert!(!like(&non_matching, &pattern, None));
        let matching = "a".repeat(5000) + "b";
        assert!(like(&matching, &pattern, None));
        // Full alphabet soup still behaves.
        assert!(like("abcXYZ", "%b%Y%", None));
        assert!(!like("abcXYZ", "%b%Q%", None));
    }

    #[test]
    fn regex_cache_reuses_and_stays_bounded() {
        let first = cached_build_regex("a.*b", "").expect("valid pattern");
        let second = cached_build_regex("a.*b", "").expect("valid pattern");
        assert!(first.is_match("axxb") && second.is_match("axxb"));
        // Flags distinguish entries.
        assert!(cached_build_regex("abc", "i")
            .expect("valid")
            .is_match("ABC"));
        assert!(!cached_build_regex("abc", "")
            .expect("valid")
            .is_match("ABC"));
        // Errors are errors, and never poison the cache.
        assert!(cached_build_regex("([", "").is_err());
        assert!(cached_build_regex("ok.*", "").is_ok());
        // An adversarial stream of distinct patterns cannot grow the table
        // without bound; evicted entries simply recompile on next use.
        for i in 0..70 {
            cached_build_regex(&format!("cap-test-{i}-.*"), "").expect("valid pattern");
        }
        assert!(regex_cache_len() <= 64, "cache must stay bounded");
        assert!(cached_build_regex("cap-test-0-.*", "")
            .expect("valid")
            .is_match("cap-test-0-!"));
    }
}
