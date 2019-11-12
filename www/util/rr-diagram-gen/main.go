// Copyright 2015 - 2019 The Cockroach Authors. All rights reserved.
// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.
//
// This file is derived from the docgen tool in CockroachDB. The
// original files were retrieved on Nov 12, 2019 from:
//
//     https://github.com/cockroachdb/cockroach/tree/d2f7fbf5dd1fc1a099bbad790a2e1f7c60a66cc3/pkg/cmd/docgen
//
// The original source code is subject to the terms of the Apache
// 2.0 license, a copy of which can be found in the LICENSE file at the
// root of this repository.

package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html"
	"golang.org/x/sys/unix"
)

const (
	rrAddr = "http://bottlecaps.de/rr/ui"
)

// EBNFtoXHTML generates the railroad XHTML from a EBNF file.
func EBNFtoXHTML(bnf []byte) ([]byte, error) {

	v := url.Values{}
	v.Add("frame", "diagram")
	v.Add("text", string(bnf))
	v.Add("width", "620")
	v.Add("options", "eliminaterecursion")
	v.Add("options", "factoring")
	v.Add("options", "inline")

	resp, err := http.Post(rrAddr, "application/x-www-form-urlencoded", strings.NewReader(v.Encode()))
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s: %s", resp.Status, string(body))
	}
	return body, nil
}

// XHTMLtoHTML converts the XHTML railroad diagrams to HTML.
func XHTMLtoHTML(xhtml []byte) (string, error) {
	r := bytes.NewReader(xhtml)
	b := new(bytes.Buffer)
	z := html.NewTokenizer(r)
	for {
		tt := z.Next()
		if tt == html.ErrorToken {
			err := z.Err()
			if err == io.EOF {
				break
			}
			return "", z.Err()
		}
		t := z.Token()
		switch t.Type {
		case html.StartTagToken, html.EndTagToken, html.SelfClosingTagToken:
			idx := strings.IndexByte(t.Data, ':')
			t.Data = t.Data[idx+1:]
		}
		var na []html.Attribute
		for _, a := range t.Attr {
			if strings.HasPrefix(a.Key, "xmlns") {
				continue
			}
			na = append(na, a)
		}
		t.Attr = na
		b.WriteString(t.String())
	}

	doc, err := goquery.NewDocumentFromReader(b)
	if err != nil {
		return "", err
	}
	defs := doc.Find("defs")
	dhtml, err := defs.First().Html()
	if err != nil {
		return "", err
	}
	doc.Find("head").AppendHtml(dhtml)
	defs.Remove()
	doc.Find("svg").First().Remove()
	doc.Find("meta[http-equiv]").Remove()
	doc.Find("head").PrependHtml(`<meta charset="UTF-8">`)
	doc.Find("a[name]:not([href])").Each(func(_ int, s *goquery.Selection) {
		name, exists := s.Attr("name")
		if !exists {
			return
		}
		s.SetAttr("href", "#"+name)
	})
	s, err := doc.Find("html").Html()
	s = "<!DOCTYPE html><html>" + s + "</html>"
	return s, err
}

// ExtractSVGDiagram extracts the embedded SVG diagram.
func ExtractSVGDiagram(html string) (string, error) {

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return "", err
	}

	// The railroad diagram page we received has two SVGs;
	// we want the first one.
	svgSelection := doc.Find("body svg").Eq(0)

	svgSelection.Children().Each(func(i int, s *goquery.Selection) {
		// Remove all anchor tags, which Materialize
		// doesn't use.
		if s.Is("a") {
			s.ReplaceWithSelection(s.Children())
		}
	})

	svgString, err := goquery.OuterHtml(svgSelection)

	if err != nil {
		return "", err
	}

	svgString += "\n"

	return svgString, nil
}

// ConvertBNFtoSVG finds all .bnf files in srcDir,
// converts them to SVG files, and writes those SVG
// files to dstDir as .html files appropriate for
// being included in Hugo.
func ConvertBNFtoSVG(srcDir string, dstDir string) {
	bnfFilename := regexp.MustCompile(`(.+?)\.bnf$`)
	files, err := ioutil.ReadDir(srcDir)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Writing converted BNF files to...")

	// Find all BNF files.
	for _, f := range files {

		if bnfFilename.Match([]byte(f.Name())) {

			bnf, err := ioutil.ReadFile(srcDir + f.Name())
			if err != nil {
				log.Fatalf(fmt.Sprintf("Cannot read %s\n", f.Name()))
			}

			xhtml, err := EBNFtoXHTML(bnf)
			if err != nil {
				panic(fmt.Sprintf("EBNF to XHTML conversion failed for %s: %v\n", f.Name(), err))
			}

			html, err := XHTMLtoHTML(xhtml)
			if err != nil {
				panic(fmt.Sprintf("XHTML to HTML conversion failed for %s: %v", f.Name(), err))
			}

			svg, err := ExtractSVGDiagram(html)
			if err != nil {
				panic(fmt.Sprintf("Extracting SVG from HTML failed for %s: %v", f.Name(), err))
			}

			dstFilename := bnfFilename.ReplaceAllString(f.Name(), dstDir+"$1.html")
			err = ioutil.WriteFile(dstFilename, []byte(svg), 0644)
			if err != nil {
				panic(fmt.Sprintf("Failed to write %s: %v", dstFilename, err))
			}
			fmt.Println("\t", dstFilename)
		}
	}
}

func main() {

	if len(os.Args) != 3 {
		log.Fatalf("USAGE: rr-diagram-gen <srcDir> <dstDir>\n")
	}

	srcDir := os.Args[1]
	dstDir := os.Args[2]

	if _, err := os.Stat(srcDir); os.IsNotExist(err) {
		log.Fatalf("srcDir (%s) does not exist.\n", srcDir)
	}

	if _, err := os.Stat(dstDir); os.IsNotExist(err) {
		log.Fatalf("dstDir (%s) does not exist.\n", srcDir)
	}

	if unix.Access(dstDir, unix.W_OK) != nil {
		log.Fatalf("dstDir (%s) is not writable\n", dstDir)
	}

	ConvertBNFtoSVG(srcDir, dstDir)

}
