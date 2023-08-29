# JsonSplitter

JsonSplitter is a small go-application which takes a .json file containing an array and splits it into smaller arrays.

Example:
Input `file.json`:
```json
[
  {"key": "value1"},
  {"key": "value2"}
]
```

Run it through the splitter:
```bash
./jsonSplitter -input ./file.json -length 1
```

You now have a directory called "output" containing 2 json files:
`file1.json`:
```json
[
  {"key":  "value1"}
]
```

`file2.json`
```json
[
  {"key":  "value2"}
]
```
