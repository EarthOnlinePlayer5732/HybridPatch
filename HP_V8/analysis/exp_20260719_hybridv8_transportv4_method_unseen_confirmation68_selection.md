# exp_20260719_hybridv8_transportv4_method_unseen_confirmation68 selection

This is a zero-API split audit. It does not read provider payload bodies and it is not an effect experiment.

## Exposure conclusion

- Strict any-provider exposure: 234/234 exposed; strict unseen candidates = 0.
- Repository method/developer-unseen policy: 149 excluded; 85 runnable candidates.
- FR-only frozen FullRewrite control calls are reported as strict exposure but are not treated as HP method exposure by this repository holdout policy.

## Selection

- Seed: `42`
- Rule: `candidate_count_below100_select_about80_percent`
- Selected: `68`
- Reserve: `17`

### Selected sample IDs

```text
audiosyn1 chess4 circuit5 dbschema3 dbschema6 docker5 earncall2 earncall3 earncall5 earncall6 edifact1 filesystem4 filesystem6 fonteng6 foodmenu2 foodmenu3 foodmenu4 genealogy5 geotrack1 geotrack2 geotrack4 graphviz5 hamradio2 hamradio5 infra3 infra4 infra6 jobboard2 jobboard4 json1 json5 landmarks5 latex3 libcatalog3 makefile2 makefile3 malware1 malware2 malware5 mathlean6 molecule1 musicsheet3 musicsheet4 obj3d1 obj3d3 obj3d6 protein2 protein4 protein5 protein6 python4 quantum3 satellite1 satellite2 satellite5 spreadsheet2 spreadsheet4 starcatalog1 subtitles4 transit3 transit5 translation1 translation3 translation6 treebank1 treebank5 weather2 weather6
```

### Reserve sample IDs

```text
audiosyn4 circuit4 earncall4 filesystem5 graphviz4 jobboard6 landmarks4 latex4 makefile1 malware4 molecule4 molecule5 satellite3 spreadsheet3 subtitles5 translation5 weather3
```

## Coverage summary

### candidate

- domain: audiosyn:2, chess:1, circuit:2, dbschema:2, docker:1, earncall:5, edifact:1, filesystem:3, fonteng:1, foodmenu:3, genealogy:1, geotrack:3, graphviz:2, hamradio:2, infra:3, jobboard:3, json:2, landmarks:2, latex:2, libcatalog:1, makefile:3, malware:4, mathlean:1, molecule:3, musicsheet:2, obj3d:3, protein:4, python:1, quantum:1, satellite:4, spreadsheet:3, starcatalog:1, subtitles:2, transit:2, translation:4, treebank:2, weather:3
- format: .adi:2, .bib:2, .cir:2, .conllu:2, .csd:2, .csv:3, .dot:2, .edi:1, .fea:1, .ged:1, .gpx:3, .json:2, .kml:2, .lean:1, .ly:2, .mk:3, .mtl:3, .obj:3, .pdb:4, .pgn:1, .po:4, .py:1, .qasm:1, .sdf:3, .sql:2, .srt:2, .tex:2, .tf:3, .tle:4, .txt:19, .xml:2, .yar:4, [none]:1
- task_type: classification:77, constraint_satisfaction:21, context_expansion:45, domain_knowledge:20, format_knowledge:66, global_restructure:1, local_edit:1, numerical_reasoning:66, referencing:52, sorting:78, split_and_merge:81, string_manipulation:65, topic_modeling:13
- file_count: 1:77, 2:6, 3+:2
- doc_length: q1:17, q2:17, q3:17, q4:17, q5:17

### selected

- domain: audiosyn:1, chess:1, circuit:1, dbschema:2, docker:1, earncall:4, edifact:1, filesystem:2, fonteng:1, foodmenu:3, genealogy:1, geotrack:3, graphviz:1, hamradio:2, infra:3, jobboard:2, json:2, landmarks:1, latex:1, libcatalog:1, makefile:2, malware:3, mathlean:1, molecule:1, musicsheet:2, obj3d:3, protein:4, python:1, quantum:1, satellite:3, spreadsheet:2, starcatalog:1, subtitles:1, transit:2, translation:3, treebank:2, weather:2
- format: .adi:2, .bib:1, .cir:1, .conllu:2, .csd:1, .csv:2, .dot:1, .edi:1, .fea:1, .ged:1, .gpx:3, .json:2, .kml:1, .lean:1, .ly:2, .mk:2, .mtl:3, .obj:3, .pdb:4, .pgn:1, .po:3, .py:1, .qasm:1, .sdf:1, .sql:2, .srt:1, .tex:1, .tf:3, .tle:3, .txt:15, .xml:2, .yar:3, [none]:1
- task_type: classification:62, constraint_satisfaction:17, context_expansion:36, domain_knowledge:16, format_knowledge:53, global_restructure:1, local_edit:1, numerical_reasoning:53, referencing:42, sorting:63, split_and_merge:65, string_manipulation:52, topic_modeling:10
- file_count: 1:61, 2:5, 3+:2
- doc_length: q1:14, q2:13, q3:14, q4:14, q5:13

### reserve

- domain: audiosyn:1, circuit:1, earncall:1, filesystem:1, graphviz:1, jobboard:1, landmarks:1, latex:1, makefile:1, malware:1, molecule:2, satellite:1, spreadsheet:1, subtitles:1, translation:1, weather:1
- format: .bib:1, .cir:1, .csd:1, .csv:1, .dot:1, .kml:1, .mk:1, .po:1, .sdf:2, .srt:1, .tex:1, .tle:1, .txt:4, .yar:1
- task_type: classification:15, constraint_satisfaction:4, context_expansion:9, domain_knowledge:4, format_knowledge:13, numerical_reasoning:13, referencing:10, sorting:15, split_and_merge:16, string_manipulation:13, topic_modeling:3
- file_count: 1:16, 2:1
- doc_length: q1:3, q2:4, q3:3, q4:3, q5:4

## Runtime evaluator smoke

- Checked: `234`; runnable: `234`; failed: `0`.
- Non-unit reference scores: makefile1:0.95, makefile2:0.95, makefile3:0.95, makefile4:0.95, makefile6:0.95, obj3d5:0.9129, transit3:0.9941747572815534

## Input digests

- selector_script: `a597eec52435575baf41f0bbe2450e923ddcad6675de7d1efef8748d08a48398`
- registry: `c2e622f27e712c5cb3fbba06ae0a34d944e4d889d7658933d3b95f0ee222f305`
- hybrid_split: `f05c92f374f45e6d0caf2a3cfad7cd86cd776163d2cf4286f4dbf446fc50e87f`
- sample_json_manifest: `c4017f9d8062aa3dc96b6c2f28b0ed6f51727b783b973b89e7f32208e665443a`
- strict provider-evidence manifest: `da6d58211475269ec3c0d2de0bdd9aeca52d80772e5cd9eaa38592bf4767447e`
- method-evidence manifest: `2a3a8d2e75e21c3ad524175f5125f95c0e73bc9234afc02344ef7638463fcd3c`
- selection_preview: `0f1a793550d4c4cbe8450bb7d1640b95e4d54cbd1b51fd0e16dff9cf6fb911bb`
- Dynamic generation metadata is omitted for byte reproducibility.
- Formal API launch must use the committed selection artifact from a clean tree.

## Caveats

- Strict any-provider exposure leaves zero unseen samples because the frozen FullRewrite baseline has raw API evidence for all 234 delegate52 samples.
- The selected confirmation set is method/developer-unseen under the repository holdout policy, not absolute provider-unseen.
- The sealed dev, val, test, and unused-reserve splits are all excluded; in particular, the 2026-07-03 live HybridPatch test20 cannot re-enter through a stale registry label.
- python1 is excluded because docs/FINDINGS.md:370-399 records a developer-level task/evaluator investigation even though the older registry still labels it clean_candidate.
- audiosyn1 and audiosyn4 retain the registry's historical unsupported_generation_domain annotation but are eligible because the user-specified current evaluator runtime criterion passes; the annotation remains disclosed.
- Runtime-runnable means the current evaluator entry returns a finite score without local/API exceptions; it does not prove every evaluator is discriminative.
- This tool performs zero API calls and is not an effect experiment.
