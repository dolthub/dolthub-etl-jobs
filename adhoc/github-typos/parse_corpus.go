package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type inputJson struct {
	Repo    string `json:"repo"`
	Commit  string `json:"commit"`
	Message string `json:"message"`
	Edits   []inputEditJson `json:"edits"`
}

type inputEditJson struct {
	Source          inputEditDetailsJson `json:"src"`
	Target          inputEditDetailsJson `json:"tgt"`
	ProbabilityTypo float64              `json:"prob_typo"`
	IsTypo          bool                 `json:"is_typo"`
}

type inputEditDetailsJson struct {
	Text       string  `json:"text"`
	Path       string  `json:"path"`
	Lang       string  `json:"lang"`
	Perplexity float64 `json:"ppl"`
}

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

	jsonData, err := os.Open(jsonFile)
	if err != nil {
		panic(err)
	}

	decoder := json.NewDecoder(jsonData)
	for decoder.More() {
		line := inputJson{}
		if err := decoder.Decode(&line); err != nil {
			panic(err)
		}
		emitObj(line)
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

func emitObj(line inputJson) {
	key := commitKey{
		Repo:   line.Repo,
		Commit: line.Commit,
	}

	if !seenCommits[key] {
		commits = append(commits, &outputJsonCommit{
			Repo:    line.Repo,
			Commit:  line.Commit,
			Message: line.Message,
		})
	}

	seenCommits[key] = true

	for i, edit := range line.Edits {
		edits = append(edits, &outputJsonEdit{
			Repo:             line.Repo,
			Commit:           line.Commit,
			EditNumber:       i+1,
			SourceText:       edit.Source.Text,
			SourcePath:       edit.Source.Path,
			SourceLang:       edit.Source.Lang,
			SourcePerplexity: edit.Source.Perplexity,
			TargetText:       edit.Target.Text,
			TargetPath:       edit.Target.Path,
			TargetLang:       edit.Target.Lang,
			TargetPerplexity: edit.Target.Perplexity,
			ProbTypo:         edit.ProbabilityTypo,
			IsTypo:           edit.IsTypo,
		})
	}
}