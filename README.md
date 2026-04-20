# CCBD_Project6

Variant 6 — Cost (“cloud bill”) estimator
Goal. Build a simple, transparent “cloud bill” estimator for object storage usage and demonstrate how
data engineering choices (format/layout/compression/file sizing) affect cost.
What you must implement.
• Use a pricing model (see Section A.6.1), which include: storage per GB-month, request costs
(PUT/GET/LIST), and transfer costs (egress).
• Instrument your pipeline to estimate:
– Total stored bytes (by listing objects and summing sizes).
– Request counts (at least approximate): number of PUTs/GETs/LISTs performed by your scripts.
– Data transferred (approximate): bytes uploaded/downloaded by your scripts.
• Compute an estimated cost for the pipeline execution and for storing the dataset.
Experiments (run for S/M/L).
• Compute the bill for at least two design choices, e.g.: snappy vs zstd; small files vs compact; partitionby-date vs flat.
• Provide a cost breakdown (storage vs requests vs transfer).
Expected discussion. Explain which cost component dominates and why. Provide concrete recommendations to reduce cost while keeping acceptable performance. State clearly what is measured vs approximated
(threats to validity).
