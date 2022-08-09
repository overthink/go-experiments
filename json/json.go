package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

// Navigate the JSON without defining a bunch of structs
func unstructured(jsonBytes []byte) {
	var parsed map[string]interface{}
	json.Unmarshal(jsonBytes, &parsed)
	tracks := parsed["lovedtracks"].(map[string]interface{})["track"].([]interface{})
	fmt.Printf("found %d tracks\n", len(tracks))
}

func structured(jsonBytes []byte) {
	type artist struct {
		Name string `json:"name"`
	}
	type track struct {
		Artist artist `json:"artist"`
		Name   string `json:"name"`
	}
	type lovedtracks struct {
		Tracks []track `json:"track"`
	}
	type doc struct {
		LovedTracks lovedtracks `json:"lovedtracks"`
	}
	var result doc
	json.Unmarshal(jsonBytes, &result)
	fmt.Printf("found %d tracks\n", len(result.LovedTracks.Tracks))
}

func main() {
	f, err := os.Open("loved_tracks.json")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	bytes, err := io.ReadAll(f)
	if err != nil {
		log.Fatal(err)
	}
	structured(bytes)
	unstructured(bytes)
}
