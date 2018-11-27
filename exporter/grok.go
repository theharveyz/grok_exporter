// Copyright 2016-2018 The grok_exporter Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package exporter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/fstab/grok_exporter/config/v2"
	"github.com/fstab/grok_exporter/oniguruma"
	"github.com/fstab/grok_exporter/template"
)

// Compile a grok pattern string into a regular expression.
func Compile(pattern string, patterns *Patterns) (*oniguruma.Regex, error) {
	regex, err := expand(pattern, patterns)
	if err != nil {
		return nil, err
	}
	result, err := oniguruma.Compile(regex)
	if err != nil {
		return nil, fmt.Errorf("failed to compile pattern %v: error in regular expression %v: %v", pattern, regex, err.Error())
	}
	return result, nil
}

func VerifyFieldNames(metric string, m *v2.MetricMatch, regex, deleteRegex *oniguruma.Regex) error {
	for _, template := range m.LabelTemplates {
		err := verifyFieldName(metric, template, regex)
		if err != nil {
			return err
		}
	}
	for _, template := range m.DeleteLabelTemplates {
		err := verifyFieldName(metric, template, deleteRegex)
		if err != nil {
			return err
		}
	}
	if m.ValueTemplate != nil {
		err := verifyFieldName(metric, m.ValueTemplate, regex)
		if err != nil {
			return err
		}
	}
	return nil
}

func verifyFieldName(metricName string, template template.Template, regex *oniguruma.Regex) error {
	if template != nil {
		for _, grokFieldName := range template.ReferencedGrokFields() {
			if !regex.HasCaptureGroup(grokFieldName) {
				return fmt.Errorf("%v: grok field %v not found in match pattern", metricName, grokFieldName)
			}
		}
	}
	return nil
}

// PATTERN_RE matches the %{..} patterns. There are three possibilities:
// 1) %{USER}               - grok pattern
// 2) %{IP:clientip}        - grok pattern with name
// 3) %{INT:clientport:int} - grok pattern with name and type (type is currently ignored)
const PATTERN_RE = `%{(.+?)}`

// Expand recursively resolves all grok patterns %{..} and returns a regular expression.
func expand(pattern string, patterns *Patterns) (string, error) {
	result := pattern
	for i := 0; i < 1000; i++ { // After 1000 replacements, we assume this is an infinite loop and abort.
		match := regexp.MustCompile(PATTERN_RE).FindStringSubmatch(result)
		if match == nil {
			// No match means all grok patterns %{..} are expanded. We are done.
			return result, nil
		}
		parts := strings.Split(match[1], ":")
		regex, exists := patterns.Find(parts[0])
		if !exists {
			return "", fmt.Errorf("Pattern %v not defined.", match[0])
		}
		var replacement string
		switch {
		case len(parts) == 1:
			// If the grok pattern has no name, we don't need to capture, so we use ?:
			replacement = fmt.Sprintf("(?:%v)", regex)
		case len(parts) == 2 || len(parts) == 3:
			// If the grok pattern has a name, we create a named capturing group with ?<>
			replacement = fmt.Sprintf("(?<%v>%v)", parts[1], regex)
		default:
			return "", fmt.Errorf("%v is not a valid pattern.", match[0])
		}
		result = strings.Replace(result, match[0], replacement, -1)
	}
	return "", fmt.Errorf("Deep recursion while expanding pattern '%v'.", pattern)
}
