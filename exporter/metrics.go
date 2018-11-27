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
	"strconv"
	"time"

	configuration "github.com/fstab/grok_exporter/config/v2"
	"github.com/fstab/grok_exporter/oniguruma"
	"github.com/fstab/grok_exporter/template"
	"github.com/prometheus/client_golang/prometheus"
)

type Match struct {
	Labels map[string]string
	Value  float64
}

type Metric interface {
	Name() string
	Collector() prometheus.Collector

	// Returns the match if the line matched, and nil if the line didn't match.
	ProcessMatch(line string) (*Match, error)
	// Returns the match if the delete pattern matched, nil otherwise.
	ProcessDeleteMatch(line string) (*Match, error)
	// Remove old metrics
	ProcessRetention() error
}

// Common values for incMetric and observeMetric
type metric struct {
	name         string
	regexs       map[string]*oniguruma.Regex
	deleteRegexs map[string]*oniguruma.Regex
	retention    time.Duration
}

type observeMetric struct {
	metric
	valueTemplates map[string]template.Template
}

type metricWithLabels struct {
	metric
	labelTemplates       map[string][]template.Template
	deleteLabelTemplates map[string][]template.Template
	labelValueTracker    LabelValueTracker
}

type observeMetricWithLabels struct {
	metricWithLabels
	valueTemplates map[string]template.Template
}

type counterMetric struct {
	metric
	counter prometheus.Counter
}

type counterVecMetric struct {
	metricWithLabels
	counterVec *prometheus.CounterVec
}

type gaugeMetric struct {
	observeMetric
	cumulative bool
	gauge      prometheus.Gauge
}

type gaugeVecMetric struct {
	observeMetricWithLabels
	cumulative bool
	gaugeVec   *prometheus.GaugeVec
}

type histogramMetric struct {
	observeMetric
	histogram prometheus.Histogram
}

type histogramVecMetric struct {
	observeMetricWithLabels
	histogramVec *prometheus.HistogramVec
}

type summaryMetric struct {
	observeMetric
	summary prometheus.Summary
}

type summaryVecMetric struct {
	observeMetricWithLabels
	summaryVec *prometheus.SummaryVec
}

type deleterMetric interface {
	Delete(prometheus.Labels) bool
}

func (m *metric) Name() string {
	return m.name
}

func (m *counterMetric) Collector() prometheus.Collector {
	return m.counter
}

func (m *counterVecMetric) Collector() prometheus.Collector {
	return m.counterVec
}

func (m *gaugeMetric) Collector() prometheus.Collector {
	return m.gauge
}

func (m *gaugeVecMetric) Collector() prometheus.Collector {
	return m.gaugeVec
}

func (m *histogramMetric) Collector() prometheus.Collector {
	return m.histogram
}

func (m *histogramVecMetric) Collector() prometheus.Collector {
	return m.histogramVec
}

func (m *summaryMetric) Collector() prometheus.Collector {
	return m.summary
}

func (m *summaryVecMetric) Collector() prometheus.Collector {
	return m.summaryVec
}

func (m *metric) processMatch(line string, cb func()) (*Match, error) {
	var err error
	for _, reg := range m.regexs {
		searchResult, e := reg.Search(line)
		if e != nil {
			err = fmt.Errorf("error processing metric %v: %v", m.Name(), e.Error())
			continue
		}
		defer searchResult.Free()
		if searchResult.IsMatch() {
			cb()
			return &Match{
				Value: 1.0,
			}, nil
		}
	}
	return nil, err

}

func (m *observeMetric) processMatch(line string, cb func(value float64)) (*Match, error) {
	var err error
	for name, reg := range m.regexs {
		searchResult, e := reg.Search(line)
		if e != nil {
			err = fmt.Errorf("error processing metric %v: %v", m.Name(), e.Error())
			continue
		}
		defer searchResult.Free()
		if searchResult.IsMatch() {
			floatVal, err := floatValue(m.Name(), searchResult, m.valueTemplates[name])
			if err != nil {
				return nil, err
			}
			cb(floatVal)
			return &Match{
				Value: floatVal,
			}, nil
		}
	}
	return nil, err
}

func (m *metricWithLabels) processMatch(line string, cb func(labels map[string]string)) (*Match, error) {
	var err error
	for name, reg := range m.regexs {
		searchResult, e := reg.Search(line)
		if e != nil {
			err = fmt.Errorf("error while processing metric %v: %v", m.Name(), e.Error())
			continue
		}
		defer searchResult.Free()
		if searchResult.IsMatch() {
			labels, err := labelValues(m.Name(), searchResult, m.labelTemplates[name])
			if err != nil {
				return nil, err
			}
			m.labelValueTracker.Observe(labels)
			cb(labels)
			return &Match{
				Value:  1.0,
				Labels: labels,
			}, nil
		}
	}
	return nil, err
}

func (m *observeMetricWithLabels) processMatch(line string, cb func(value float64, labels map[string]string)) (*Match, error) {
	var err error
	for name, reg := range m.regexs {
		searchResult, e := reg.Search(line)
		if e != nil {
			err = fmt.Errorf("error processing metric %v: %v", m.Name(), e.Error())
			continue
		}
		defer searchResult.Free()
		if searchResult.IsMatch() {
			floatVal, err := floatValue(m.Name(), searchResult, m.valueTemplates[name])
			if err != nil {
				return nil, err
			}
			labels, err := labelValues(m.Name(), searchResult, m.labelTemplates[name])
			if err != nil {
				return nil, err
			}
			m.labelValueTracker.Observe(labels)
			cb(floatVal, labels)
			return &Match{
				Value:  floatVal,
				Labels: labels,
			}, nil
		}
	}
	return nil, err
}

func (m *metric) ProcessDeleteMatch(line string) (*Match, error) {
	return nil, nil
	// return nil, fmt.Errorf("error processing metric %v: delete_match is currently only supported for metrics with labels.", m.Name())
}

func (m *metric) ProcessRetention() error {
	return nil
	// if m.retention == 0 {
	// }
	// return fmt.Errorf("error processing metric %v: retention is currently only supported for metrics with labels.", m.Name())
}

func (m *metricWithLabels) processDeleteMatch(line string, vec deleterMetric) (*Match, error) {
	var err error
	for name, reg := range m.deleteRegexs {
		searchResult, e := reg.Search(line)
		if e != nil {
			err = fmt.Errorf("error processing metric %v: %v", m.Name(), e.Error())
			continue
		}
		defer searchResult.Free()
		if searchResult.IsMatch() {
			deleteLabels, err := labelValues(m.Name(), searchResult, m.deleteLabelTemplates[name])
			if err != nil {
				return nil, err
			}
			matchingLabels, err := m.labelValueTracker.DeleteByLabels(deleteLabels)
			if err != nil {
				return nil, err
			}
			for _, matchingLabel := range matchingLabels {
				vec.Delete(matchingLabel)
			}
			return &Match{
				Labels: deleteLabels,
			}, nil
		}
	}
	return nil, err
}

func (m *metricWithLabels) processRetention(vec deleterMetric) error {
	if m.retention != 0 {
		for _, label := range m.labelValueTracker.DeleteByRetention(m.retention) {
			vec.Delete(label)
		}
	}
	return nil
}

func (m *counterMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, func() {
		m.counter.Inc()
	})
}

func (m *counterVecMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, func(labels map[string]string) {
		m.counterVec.With(labels).Inc()
	})
}

func (m *counterVecMetric) ProcessDeleteMatch(line string) (*Match, error) {
	return m.processDeleteMatch(line, m.counterVec)
}

func (m *counterVecMetric) ProcessRetention() error {
	return m.processRetention(m.counterVec)
}

func (m *gaugeMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, func(value float64) {
		if m.cumulative {
			m.gauge.Add(value)
		} else {
			m.gauge.Set(value)
		}
	})
}

func (m *gaugeVecMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, func(value float64, labels map[string]string) {
		if m.cumulative {
			m.gaugeVec.With(labels).Add(value)
		} else {
			m.gaugeVec.With(labels).Set(value)
		}
	})
}

func (m *gaugeVecMetric) ProcessDeleteMatch(line string) (*Match, error) {
	return m.processDeleteMatch(line, m.gaugeVec)
}

func (m *gaugeVecMetric) ProcessRetention() error {
	return m.processRetention(m.gaugeVec)
}

func (m *histogramMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, func(value float64) {
		m.histogram.Observe(value)
	})
}

func (m *histogramVecMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, func(value float64, labels map[string]string) {
		m.histogramVec.With(labels).Observe(value)
	})
}

func (m *histogramVecMetric) ProcessDeleteMatch(line string) (*Match, error) {
	return m.processDeleteMatch(line, m.histogramVec)
}

func (m *histogramVecMetric) ProcessRetention() error {
	return m.processRetention(m.histogramVec)
}

func (m *summaryMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, func(value float64) {
		m.summary.Observe(value)
	})
}

func (m *summaryVecMetric) ProcessMatch(line string) (*Match, error) {
	return m.processMatch(line, func(value float64, labels map[string]string) {
		m.summaryVec.With(labels).Observe(value)
	})
}

func (m *summaryVecMetric) ProcessDeleteMatch(line string) (*Match, error) {
	return m.processDeleteMatch(line, m.summaryVec)
}

func (m *summaryVecMetric) ProcessRetention() error {
	return m.processRetention(m.summaryVec)
}

func newMetric(cfg *configuration.MetricConfig, regexs, deleteRegexs map[string]*oniguruma.Regex) metric {
	return metric{
		name:         cfg.Name,
		regexs:       regexs,
		deleteRegexs: deleteRegexs,
		retention:    cfg.Retention,
	}
}

func newMetricWithLabels(cfg *configuration.MetricConfig, regex, deleteRegex map[string]*oniguruma.Regex) metricWithLabels {
	lt := make(map[string][]template.Template)
	dlt := make(map[string][]template.Template)
	for _, match := range cfg.Matchs {
		lt[match.Match] = match.LabelTemplates
		dlt[match.Match] = match.DeleteLabelTemplates
	}

	return metricWithLabels{
		metric:               newMetric(cfg, regex, deleteRegex),
		labelTemplates:       lt,
		deleteLabelTemplates: dlt,
		labelValueTracker:    NewLabelValueTracker(prometheusLabels(lt)),
	}
}

func newObserveMetric(cfg *configuration.MetricConfig, regexs, deleteRegexs map[string]*oniguruma.Regex) observeMetric {
	vt := make(map[string]template.Template)
	for _, match := range cfg.Matchs {
		vt[match.Match] = match.ValueTemplate
	}

	return observeMetric{
		metric:         newMetric(cfg, regexs, deleteRegexs),
		valueTemplates: vt,
	}
}

func newObserveMetricWithLabels(cfg *configuration.MetricConfig, regexs, deleteRegexs map[string]*oniguruma.Regex) observeMetricWithLabels {
	vt := make(map[string]template.Template)
	for _, match := range cfg.Matchs {
		vt[match.Match] = match.ValueTemplate
	}
	return observeMetricWithLabels{
		metricWithLabels: newMetricWithLabels(cfg, regexs, deleteRegexs),
		valueTemplates:   vt,
	}
}

func NewCounterMetric(cfg *configuration.MetricConfig, regexs, deleteRegexs map[string]*oniguruma.Regex) Metric {
	counterOpts := prometheus.CounterOpts{
		Name: cfg.Name,
		Help: cfg.Help,
	}
	if len(getLabels(cfg)) == 0 {
		return &counterMetric{
			metric:  newMetric(cfg, regexs, deleteRegexs),
			counter: prometheus.NewCounter(counterOpts),
		}
	}
	lt := make(map[string][]template.Template)
	for _, match := range cfg.Matchs {
		lt[match.Match] = match.LabelTemplates
	}
	return &counterVecMetric{
		metricWithLabels: newMetricWithLabels(cfg, regexs, deleteRegexs),
		counterVec:       prometheus.NewCounterVec(counterOpts, prometheusLabels(lt)),
	}
}

func getLabels(cfg *configuration.MetricConfig) map[string]string {
	lbs := map[string]string{}
	for _, m := range cfg.Matchs {
		for k, l := range m.Labels {
			lbs[k] = l
		}
	}
	return lbs
}

func NewGaugeMetric(cfg *configuration.MetricConfig, regexs, deleteRegexs map[string]*oniguruma.Regex) Metric {
	gaugeOpts := prometheus.GaugeOpts{
		Name: cfg.Name,
		Help: cfg.Help,
	}

	if len(getLabels(cfg)) == 0 {
		return &gaugeMetric{
			observeMetric: newObserveMetric(cfg, regexs, deleteRegexs),
			cumulative:    cfg.Cumulative,
			gauge:         prometheus.NewGauge(gaugeOpts),
		}
	} else {
		lt := make(map[string][]template.Template)
		for _, match := range cfg.Matchs {
			lt[match.Match] = match.LabelTemplates
		}
		return &gaugeVecMetric{
			observeMetricWithLabels: newObserveMetricWithLabels(cfg, regexs, deleteRegexs),
			cumulative:              cfg.Cumulative,
			gaugeVec:                prometheus.NewGaugeVec(gaugeOpts, prometheusLabels(lt)),
		}
	}
}

func NewHistogramMetric(cfg *configuration.MetricConfig, regexs, deleteRegexs map[string]*oniguruma.Regex) Metric {
	histogramOpts := prometheus.HistogramOpts{
		Name: cfg.Name,
		Help: cfg.Help,
	}
	if len(cfg.Buckets) > 0 {
		histogramOpts.Buckets = cfg.Buckets
	}
	if len(getLabels(cfg)) == 0 {
		return &histogramMetric{
			observeMetric: newObserveMetric(cfg, regexs, deleteRegexs),
			histogram:     prometheus.NewHistogram(histogramOpts),
		}
	} else {
		lt := make(map[string][]template.Template)
		for _, match := range cfg.Matchs {
			lt[match.Match] = match.LabelTemplates
		}
		return &histogramVecMetric{
			observeMetricWithLabels: newObserveMetricWithLabels(cfg, regexs, deleteRegexs),
			histogramVec:            prometheus.NewHistogramVec(histogramOpts, prometheusLabels(lt)),
		}
	}
}

func NewSummaryMetric(cfg *configuration.MetricConfig, regexs, deleteRegexs map[string]*oniguruma.Regex) Metric {
	summaryOpts := prometheus.SummaryOpts{
		Name: cfg.Name,
		Help: cfg.Help,
	}
	if len(cfg.Quantiles) > 0 {
		summaryOpts.Objectives = cfg.Quantiles
	}
	if len(getLabels(cfg)) == 0 {
		return &summaryMetric{
			observeMetric: newObserveMetric(cfg, regexs, deleteRegexs),
			summary:       prometheus.NewSummary(summaryOpts),
		}
	} else {
		lt := make(map[string][]template.Template)
		for _, match := range cfg.Matchs {
			lt[match.Match] = match.LabelTemplates
		}
		return &summaryVecMetric{
			observeMetricWithLabels: newObserveMetricWithLabels(cfg, regexs, deleteRegexs),
			summaryVec:              prometheus.NewSummaryVec(summaryOpts, prometheusLabels(lt)),
		}
	}
}

func labelValues(metricName string, searchResult *oniguruma.SearchResult, templates []template.Template) (map[string]string, error) {
	result := make(map[string]string, len(templates))
	fmt.Println("labelValues:", metricName, templates)
	for _, t := range templates {
		fmt.Println("labelValues:", metricName, templates)

		value, err := evalTemplate(searchResult, t)
		if err != nil {
			return nil, fmt.Errorf("error processing metric %v: %v", metricName, err.Error())
		}
		result[t.Name()] = value
	}
	fmt.Println("labelValues:", result)

	return result, nil
}

func floatValue(metricName string, searchResult *oniguruma.SearchResult, valueTemplate template.Template) (float64, error) {
	stringVal, err := evalTemplate(searchResult, valueTemplate)
	if err != nil {
		return 0, fmt.Errorf("error processing metric %v: %v", metricName, err.Error())
	}
	floatVal, err := strconv.ParseFloat(stringVal, 64)
	if err != nil {
		return 0, fmt.Errorf("error processing metric %v: value matches '%v', which is not a valid number.", metricName, stringVal)
	}
	return floatVal, nil
}

func evalTemplate(searchResult *oniguruma.SearchResult, t template.Template) (string, error) {
	grokValues := make(map[string]string, len(t.ReferencedGrokFields()))
	for _, field := range t.ReferencedGrokFields() {
		value, err := searchResult.GetCaptureGroupByName(field)
		if err != nil {
			return "", err
		}
		grokValues[field] = value
	}
	return t.Execute(grokValues)
}

func prometheusLabels(templates map[string][]template.Template) []string {

	lset := make(map[string]struct{})
	promLabels := make([]string, 0)
	for _, tt := range templates {
		for _, t := range tt {
			name := t.Name()
			if _, ok := lset[name]; ok {
				continue
			}
			lset[name] = struct{}{}
			promLabels = append(promLabels, name)
		}
	}
	fmt.Println("promLabels:", promLabels)

	return promLabels
}
