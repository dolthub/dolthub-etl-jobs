package main

import (
	"encoding/json"
	"fmt"
	"github.com/bcicen/jstream"
	"io/ioutil"
	"os"
)

type commitKey struct {
	Repo    string
	Commit  string
}

type outputJsonCommit struct {
	Repo    string `json:"repo"`
	Commit  string `json:"commit_hash"`
	Message string `json:"message"`
}

type outputJsonCommits struct {
	Rows []*outputJsonCommit `json:"rows"`
}

type outputJsonEdit struct {
	Repo             string  `json:"repo"`
	Commit           string  `json:"commit_hash"`
	EditNumber       int     `json:"edit_number"`
	SourceText       string  `json:"source_text,omitempty"`
	SourcePath       string  `json:"source_path,omitempty"`
	SourceLang       string  `json:"source_lang,omitempty"`
	SourcePerplexity float64 `json:"source_perplexity,omitempty"`
	TargetText       string  `json:"target_text,omitempty"`
	TargetPath       string  `json:"target_path,omitempty"`
	TargetLang       string  `json:"target_lang,omitempty"`
	TargetPerplexity float64 `json:"target_perplexity,omitempty"`
	ProbTypo         float64 `json:"prob_typo"`
	IsTypo           bool    `json:"is_typo"`
}

type outputJsonEdits struct {
	Rows []*outputJsonEdit `json:"rows"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: parse_corpus <input json file>")
		os.Exit(1)
	}

	jsonFile := os.Args[1]

	tblData, err := os.Open(jsonFile)
	if err != nil {
		panic(err)
	}

	decoder := jstream.NewDecoder(tblData, 0) // extract JSON values at a depth level of 1
	objs := decoder.Stream()

	for obj := range objs {
		emitObj(obj)
	}

	writeResults()
}

func writeResults() {
	commitsBytes, err := json.Marshal(outputJsonCommits{commits})
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile("commits.json", commitsBytes, os.ModePerm)
	if err != nil {
		panic(err)
	}

	editsBytes, err := json.Marshal(outputJsonEdits{edits})
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile("edits.json", editsBytes, os.ModePerm)
	if err != nil {
		panic(err)
	}
}

var seenCommits = make(map[commitKey]bool)
var commits = make([]*outputJsonCommit, 0)
var edits = make([]*outputJsonEdit, 0)

func emitObj(obj *jstream.MetaValue) {
	m := obj.Value.(map[string]interface{})
	repo := m["repo"].(string)
	commit := m["commit"].(string)
	message := m["message"].(string)

	key := commitKey{
		Repo:   repo,
		Commit: commit,
	}

	if !seenCommits[key] {
		commits = append(commits, &outputJsonCommit{
			Repo:    repo,
			Commit:  commit,
			Message: message,
		})
	}

	editList := m["edits"].([]interface{})

	seenCommits[key] = true

	for i, edit := range editList {
		editMap := edit.(map[string]interface{})
		src := editMap["src"].(map[string]interface{})
		tgt := editMap["tgt"].(map[string]interface{})

		srcPpl := 0.0
		if mapVal, ok := src["ppl"].(float64); ok {
			srcPpl = mapVal
		}

		tgtPpl := 0.0
		if mapVal, ok := tgt["ppl"].(float64); ok {
			tgtPpl = mapVal
		}

		probTypo := 0.0
		if mapVal, ok := editMap["prob_typo"].(float64); ok {
			probTypo = mapVal
		}

		isTypo := false
		if mapVal, ok := editMap["is_typo"].(bool); ok {
			isTypo = mapVal
		}

		edits = append(edits, &outputJsonEdit{
			Repo:             repo,
			Commit:           commit,
			EditNumber:       i+1,
			SourceText:       src["text"].(string),
			SourcePath:       src["path"].(string),
			SourceLang:       src["lang"].(string),
			SourcePerplexity: srcPpl,
			TargetText:       tgt["text"].(string),
			TargetPath:       tgt["path"].(string),
			TargetLang:       tgt["lang"].(string),
			TargetPerplexity: tgtPpl,
			ProbTypo:         probTypo,
			IsTypo:           isTypo,
		})
	}
}