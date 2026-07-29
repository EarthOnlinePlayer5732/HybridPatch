# exp_20260719_hybridv8_transportv4_mixed_confirmation100 selection

Zero-API, outcome-blind mixed confirmation selection.

- Candidates: 190; selected: 100; reserve: 90.
- Method/developer-unseen: 60/81 selected.
- Historical HP/method-exposed: 40/109 selected.
- Direct prior HP API samples outside recent campaigns: 17 (all included).

## Selected 60 method/developer-unseen

```text
audiosyn1 audiosyn4 chess4 circuit5 dbschema3 dbschema6 docker5 earncall2 earncall3 earncall5 earncall6 edifact1 filesystem4 filesystem6 fonteng6 foodmenu2 foodmenu4 genealogy5 geotrack1 geotrack2 geotrack4 graphviz4 graphviz5 hamradio2 infra4 infra6 jobboard2 jobboard4 jobboard6 json5 landmarks5 latex3 libcatalog3 makefile2 makefile3 malware2 malware4 malware5 mathlean6 molecule5 musicsheet3 obj3d3 obj3d6 protein2 protein5 python4 quantum3 satellite1 satellite2 satellite5 spreadsheet2 spreadsheet3 subtitles4 transit3 translation1 translation3 translation6 treebank1 weather2 weather6
```

## Selected 40 historical HP/method-exposed

```text
accounting1 accounting5 calendar5 chess2 chess3 circuit1 dbschema4 dbschema5 dns1 docker1 edifact3 edifact6 filesystem1 fonteng2 fonteng3 foodmenu5 foodmenu6 geotrack3 hamradio3 infra2 json3 json6 landmarks1 latex2 latex5 makefile4 malware6 molecule2 musicsheet1 protein1 python2 python7 quantum4 satellite6 screenplay6 starcatalog3 starcatalog4 translation2 translation4 weather5
```

## Combined selected100

```text
accounting1 accounting5 audiosyn1 audiosyn4 calendar5 chess2 chess3 chess4 circuit1 circuit5 dbschema3 dbschema4 dbschema5 dbschema6 dns1 docker1 docker5 earncall2 earncall3 earncall5 earncall6 edifact1 edifact3 edifact6 filesystem1 filesystem4 filesystem6 fonteng2 fonteng3 fonteng6 foodmenu2 foodmenu4 foodmenu5 foodmenu6 genealogy5 geotrack1 geotrack2 geotrack3 geotrack4 graphviz4 graphviz5 hamradio2 hamradio3 infra2 infra4 infra6 jobboard2 jobboard4 jobboard6 json3 json5 json6 landmarks1 landmarks5 latex2 latex3 latex5 libcatalog3 makefile2 makefile3 makefile4 malware2 malware4 malware5 malware6 mathlean6 molecule2 molecule5 musicsheet1 musicsheet3 obj3d3 obj3d6 protein1 protein2 protein5 python2 python4 python7 quantum3 quantum4 satellite1 satellite2 satellite5 satellite6 screenplay6 spreadsheet2 spreadsheet3 starcatalog3 starcatalog4 subtitles4 transit3 translation1 translation2 translation3 translation4 translation6 treebank1 weather2 weather5 weather6
```

## Caveats

- All 234 samples have prior provider exposure through the frozen FullRewrite baseline; this is not an absolute provider-unseen experiment.
- The 60-sample cohort is unseen only under the explicitly authorized HP method/developer exposure definition.
- Only 17 eligible historical samples have direct prior HP API evidence outside the recent 10+40; all 17 are included, and the remaining 23 are selected from documented method/development-exposed samples.
- The previous paired10 and supplement40 union is excluded from both cohorts.
- This selector performs zero API calls and observes no outcome from the new experiment.
