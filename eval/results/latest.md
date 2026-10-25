# CIC-IDS-2017 detection evaluation

## Dataset labels and attack sources

| File | Rows | Benign | Attack | Attack labels | Attack source IPs |
| --- | --- | --- | --- | --- | --- |
| Monday-WorkingHours.pcap_ISCX.csv | 529918 | 529918 | 0 | none | none |
| Tuesday-WorkingHours.pcap_ISCX.csv | 445909 | 432074 | 13835 | FTP-Patator=7938, SSH-Patator=5897 | FTP-Patator: 172.16.0.1 (7938); SSH-Patator: 172.16.0.1 (5897) |
| Wednesday-workingHours.pcap_ISCX.csv | 692703 | 440031 | 252672 | DoS GoldenEye=10293, DoS Hulk=231073, DoS Slowhttptest=5499, DoS slowloris=5796, Heartbleed=11 | DoS GoldenEye: 172.16.0.1 (10293); DoS Hulk: 172.16.0.1 (231073); DoS Slowhttptest: 172.16.0.1 (5499); DoS slowloris: 172.16.0.1 (5796); Heartbleed: 172.16.0.1 (11) |
| Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv | 170366 | 168186 | 2180 | Web Attack – Brute Force=1507, Web Attack – Sql Injection=21, Web Attack – XSS=652 | Web Attack – Brute Force: 172.16.0.1 (1507); Web Attack – Sql Injection: 172.16.0.1 (21); Web Attack – XSS: 172.16.0.1 (652) |
| Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv | 288602 | 288566 | 36 | Infiltration=36 | Infiltration: 192.168.10.8 (36) |
| Friday-WorkingHours-Morning.pcap_ISCX.csv | 191033 | 189067 | 1966 | Bot=1966 | Bot: 205.174.165.73 (705), 192.168.10.15 (371), 192.168.10.8 (271), 192.168.10.9 (226), 192.168.10.14 (209), 192.168.10.5 (180), 192.168.10.12 (2), 192.168.10.17 (2) |
| Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv | 286467 | 127537 | 158930 | PortScan=158930 | PortScan: 172.16.0.1 (158930) |
| Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv | 225745 | 97718 | 128027 | DDoS=128027 | DDoS: 172.16.0.1 (128024), 192.168.10.50 (3) |

## Cleaning findings

| File | Leading-space headers | Duplicate header | Blank rows dropped | Flow Bytes/s infinities | Flow Bytes/s NaNs | Timestamp resolution | Raw range | Corrected range |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Monday-WorkingHours.pcap_ISCX.csv | 70 | fwd_header_length | 0 | 373 | 64 | seconds | 2017-07-03 01:00:01 to 2017-07-03 12:59:58 | 2017-07-03 08:55:58 to 2017-07-03 17:01:34 |
| Tuesday-WorkingHours.pcap_ISCX.csv | 70 | fwd_header_length | 0 | 63 | 201 | minutes | 2017-07-04 01:00:00 to 2017-07-04 12:59:00 | 2017-07-04 08:53:00 to 2017-07-04 17:00:00 |
| Wednesday-workingHours.pcap_ISCX.csv | 70 | fwd_header_length | 0 | 289 | 1008 | minutes | 2017-07-05 01:00:00 to 2017-07-05 12:59:00 | 2017-07-05 08:42:00 to 2017-07-05 17:10:00 |
| Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv | 70 | fwd_header_length | 288602 | 115 | 20 | minutes | 2017-07-06 08:59:00 to 2017-07-06 12:59:00 | 2017-07-06 08:59:00 to 2017-07-06 12:59:00 |
| Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv | 70 | fwd_header_length | 0 | 189 | 18 | minutes | 2017-07-06 01:00:00 to 2017-07-06 05:04:00 | 2017-07-06 13:00:00 to 2017-07-06 17:04:00 |
| Friday-WorkingHours-Morning.pcap_ISCX.csv | 70 | fwd_header_length | 0 | 94 | 28 | minutes | 2017-07-07 08:59:00 to 2017-07-07 12:59:00 | 2017-07-07 08:59:00 to 2017-07-07 12:59:00 |
| Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv | 70 | fwd_header_length | 0 | 356 | 15 | minutes | 2017-07-07 01:00:00 to 2017-07-07 03:29:00 | 2017-07-07 13:00:00 to 2017-07-07 15:29:00 |
| Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv | 70 | fwd_header_length | 0 | 30 | 4 | minutes | 2017-07-07 03:30:00 to 2017-07-07 05:02:00 | 2017-07-07 15:30:00 to 2017-07-07 17:02:00 |

## Day split

| Day | Role | Windows | Benign | Attack |
| --- | --- | --- | --- | --- |
| Monday | train: benign only | 45661 | 45661 | 0 |
| Tuesday | validation/tuning | 38222 | 38095 | 127 |
| Wednesday | test: final only | 39299 | 39213 | 86 |
| Thursday | test: final only | 36790 | 36694 | 96 |
| Friday | test: final only | 35313 | 34616 | 697 |

## Window features

| Model feature |
| --- |
| flow_count |
| distinct_destination_ports |
| distinct_destination_ips |
| top_destination_port_flow_count |
| top_destination_ip_flow_count |
| total_fwd_packets |
| total_bwd_packets |
| total_fwd_bytes |
| total_bwd_bytes |
| mean_flow_duration |
| syn_flag_sum |
| rst_flag_sum |
| ack_flag_sum |
| psh_flag_sum |
| mean_packet_size |

## Tuesday-selected thresholds

| Setting | Value |
| --- | --- |
| rule.flow_count | 726.000000 |
| rule.top_destination_port_flow_count | 74.000000 |
| rule.distinct_destination_ports | 500.000000 |
| isolation_forest.anomaly_score | 0.198144 |

## Final test metrics by day

| Detector | Test day | TN | FP | FN | TP | Precision | Recall | F1 | FPR | Alerts/benign hour |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Threshold rules | Wednesday | 38343 | 870 | 26 | 60 | 0.064516 | 0.697674 | 0.118110 | 0.022187 | 98.014184 |
| Isolation Forest | Wednesday | 38486 | 727 | 15 | 71 | 0.088972 | 0.825581 | 0.160633 | 0.018540 | 79.432624 |
| Random Forest (supervised reference upper bound) | Wednesday | 39213 | 0 | 85 | 1 | 1.000000 | 0.011628 | 0.022989 | 0.000000 | 0.000000 |
| All benign baseline | Wednesday | 39213 | 0 | 86 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Thursday | 35889 | 805 | 78 | 18 | 0.021871 | 0.187500 | 0.039173 | 0.021938 | 94.307692 |
| Isolation Forest | Thursday | 36040 | 654 | 77 | 19 | 0.028232 | 0.197917 | 0.049415 | 0.017823 | 77.076923 |
| Random Forest (supervised reference upper bound) | Thursday | 36694 | 0 | 96 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| All benign baseline | Thursday | 36694 | 0 | 96 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Friday | 33895 | 721 | 610 | 87 | 0.107673 | 0.124821 | 0.115615 | 0.020829 | 89.890110 |
| Isolation Forest | Friday | 34041 | 575 | 608 | 89 | 0.134036 | 0.127690 | 0.130786 | 0.016611 | 71.648352 |
| Random Forest (supervised reference upper bound) | Friday | 34616 | 0 | 696 | 1 | 1.000000 | 0.001435 | 0.002865 | 0.000000 | 0.000000 |
| All benign baseline | Friday | 34616 | 0 | 697 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |

## All-benign accuracy context only

| Test day | All-benign accuracy | Missed attack windows |
| --- | --- | --- |
| Wednesday | 0.997812 | 86 |
| Thursday | 0.997391 | 96 |
| Friday | 0.980262 | 697 |

## Final metrics by attack type

| Detector | Test day and attack | TN | FP | FN | TP | Precision | Recall | F1 | FPR |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Threshold rules | Wednesday: DoS GoldenEye | 38343 | 870 | 0 | 8 | 0.009112 | 1.000000 | 0.018059 | 0.022187 |
| Isolation Forest | Wednesday: DoS GoldenEye | 38486 | 727 | 0 | 8 | 0.010884 | 1.000000 | 0.021534 | 0.018540 |
| Random Forest (supervised reference upper bound) | Wednesday: DoS GoldenEye | 39213 | 0 | 8 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| All benign baseline | Wednesday: DoS GoldenEye | 39213 | 0 | 8 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Wednesday: DoS Hulk | 38343 | 870 | 1 | 20 | 0.022472 | 0.952381 | 0.043908 | 0.022187 |
| Isolation Forest | Wednesday: DoS Hulk | 38486 | 727 | 2 | 19 | 0.025469 | 0.904762 | 0.049544 | 0.018540 |
| Random Forest (supervised reference upper bound) | Wednesday: DoS Hulk | 39213 | 0 | 21 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| All benign baseline | Wednesday: DoS Hulk | 39213 | 0 | 21 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Wednesday: DoS Slowhttptest | 38343 | 870 | 10 | 9 | 0.010239 | 0.473684 | 0.020045 | 0.022187 |
| Isolation Forest | Wednesday: DoS Slowhttptest | 38486 | 727 | 8 | 11 | 0.014905 | 0.578947 | 0.029062 | 0.018540 |
| Random Forest (supervised reference upper bound) | Wednesday: DoS Slowhttptest | 39213 | 0 | 19 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| All benign baseline | Wednesday: DoS Slowhttptest | 39213 | 0 | 19 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Wednesday: DoS slowloris | 38343 | 870 | 4 | 23 | 0.025756 | 0.851852 | 0.050000 | 0.022187 |
| Isolation Forest | Wednesday: DoS slowloris | 38486 | 727 | 4 | 23 | 0.030667 | 0.851852 | 0.059202 | 0.018540 |
| Random Forest (supervised reference upper bound) | Wednesday: DoS slowloris | 39213 | 0 | 26 | 1 | 1.000000 | 0.037037 | 0.071429 | 0.000000 |
| All benign baseline | Wednesday: DoS slowloris | 39213 | 0 | 27 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Wednesday: Heartbleed | 38343 | 870 | 11 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.022187 |
| Isolation Forest | Wednesday: Heartbleed | 38486 | 727 | 1 | 10 | 0.013569 | 0.909091 | 0.026738 | 0.018540 |
| Random Forest (supervised reference upper bound) | Wednesday: Heartbleed | 39213 | 0 | 11 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| All benign baseline | Wednesday: Heartbleed | 39213 | 0 | 11 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Thursday: Infiltration | 35889 | 805 | 10 | 18 | 0.021871 | 0.642857 | 0.042303 | 0.021938 |
| Isolation Forest | Thursday: Infiltration | 36040 | 654 | 9 | 19 | 0.028232 | 0.678571 | 0.054208 | 0.017823 |
| Random Forest (supervised reference upper bound) | Thursday: Infiltration | 36694 | 0 | 28 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| All benign baseline | Thursday: Infiltration | 36694 | 0 | 28 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Thursday: Web Attack – Brute Force | 35889 | 805 | 44 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.021938 |
| Isolation Forest | Thursday: Web Attack – Brute Force | 36040 | 654 | 44 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.017823 |
| Random Forest (supervised reference upper bound) | Thursday: Web Attack – Brute Force | 36694 | 0 | 44 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| All benign baseline | Thursday: Web Attack – Brute Force | 36694 | 0 | 44 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Thursday: Web Attack – Sql Injection | 35889 | 805 | 3 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.021938 |
| Isolation Forest | Thursday: Web Attack – Sql Injection | 36040 | 654 | 3 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.017823 |
| Random Forest (supervised reference upper bound) | Thursday: Web Attack – Sql Injection | 36694 | 0 | 3 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| All benign baseline | Thursday: Web Attack – Sql Injection | 36694 | 0 | 3 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Thursday: Web Attack – XSS | 35889 | 805 | 21 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.021938 |
| Isolation Forest | Thursday: Web Attack – XSS | 36040 | 654 | 21 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.017823 |
| Random Forest (supervised reference upper bound) | Thursday: Web Attack – XSS | 36694 | 0 | 21 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| All benign baseline | Thursday: Web Attack – XSS | 36694 | 0 | 21 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Friday: Bot | 33895 | 721 | 594 | 54 | 0.069677 | 0.083333 | 0.075896 | 0.020829 |
| Isolation Forest | Friday: Bot | 34041 | 575 | 592 | 56 | 0.088748 | 0.086420 | 0.087568 | 0.016611 |
| Random Forest (supervised reference upper bound) | Friday: Bot | 34616 | 0 | 648 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| All benign baseline | Friday: Bot | 34616 | 0 | 648 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Friday: DDoS | 33895 | 721 | 0 | 22 | 0.029610 | 1.000000 | 0.057516 | 0.020829 |
| Isolation Forest | Friday: DDoS | 34041 | 575 | 0 | 22 | 0.036851 | 1.000000 | 0.071082 | 0.016611 |
| Random Forest (supervised reference upper bound) | Friday: DDoS | 34616 | 0 | 22 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| All benign baseline | Friday: DDoS | 34616 | 0 | 22 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Threshold rules | Friday: PortScan | 33895 | 721 | 16 | 11 | 0.015027 | 0.407407 | 0.028986 | 0.020829 |
| Isolation Forest | Friday: PortScan | 34041 | 575 | 16 | 11 | 0.018771 | 0.407407 | 0.035889 | 0.016611 |
| Random Forest (supervised reference upper bound) | Friday: PortScan | 34616 | 0 | 26 | 1 | 1.000000 | 0.037037 | 0.071429 | 0.000000 |
| All benign baseline | Friday: PortScan | 34616 | 0 | 27 | 0 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |

## First detection latency

| Detector | Day | Attack type | Attack windows | Latency windows |
| --- | --- | --- | --- | --- |
| Threshold rules | Wednesday | DoS GoldenEye | 8 | 0 |
| Isolation Forest | Wednesday | DoS GoldenEye | 8 | 0 |
| Random Forest (supervised reference upper bound) | Wednesday | DoS GoldenEye | 8 | missed |
| All benign baseline | Wednesday | DoS GoldenEye | 8 | missed |
| Threshold rules | Wednesday | DoS Hulk | 21 | 0 |
| Isolation Forest | Wednesday | DoS Hulk | 21 | 0 |
| Random Forest (supervised reference upper bound) | Wednesday | DoS Hulk | 21 | missed |
| All benign baseline | Wednesday | DoS Hulk | 21 | missed |
| Threshold rules | Wednesday | DoS Slowhttptest | 19 | 0 |
| Isolation Forest | Wednesday | DoS Slowhttptest | 19 | 0 |
| Random Forest (supervised reference upper bound) | Wednesday | DoS Slowhttptest | 19 | missed |
| All benign baseline | Wednesday | DoS Slowhttptest | 19 | missed |
| Threshold rules | Wednesday | DoS slowloris | 27 | 1 |
| Isolation Forest | Wednesday | DoS slowloris | 27 | 1 |
| Random Forest (supervised reference upper bound) | Wednesday | DoS slowloris | 27 | 0 |
| All benign baseline | Wednesday | DoS slowloris | 27 | missed |
| Threshold rules | Wednesday | Heartbleed | 11 | missed |
| Isolation Forest | Wednesday | Heartbleed | 11 | 0 |
| Random Forest (supervised reference upper bound) | Wednesday | Heartbleed | 11 | missed |
| All benign baseline | Wednesday | Heartbleed | 11 | missed |
| Threshold rules | Thursday | Infiltration | 28 | 3 |
| Isolation Forest | Thursday | Infiltration | 28 | 3 |
| Random Forest (supervised reference upper bound) | Thursday | Infiltration | 28 | missed |
| All benign baseline | Thursday | Infiltration | 28 | missed |
| Threshold rules | Thursday | Web Attack – Brute Force | 44 | missed |
| Isolation Forest | Thursday | Web Attack – Brute Force | 44 | missed |
| Random Forest (supervised reference upper bound) | Thursday | Web Attack – Brute Force | 44 | missed |
| All benign baseline | Thursday | Web Attack – Brute Force | 44 | missed |
| Threshold rules | Thursday | Web Attack – Sql Injection | 3 | missed |
| Isolation Forest | Thursday | Web Attack – Sql Injection | 3 | missed |
| Random Forest (supervised reference upper bound) | Thursday | Web Attack – Sql Injection | 3 | missed |
| All benign baseline | Thursday | Web Attack – Sql Injection | 3 | missed |
| Threshold rules | Thursday | Web Attack – XSS | 21 | missed |
| Isolation Forest | Thursday | Web Attack – XSS | 21 | missed |
| Random Forest (supervised reference upper bound) | Thursday | Web Attack – XSS | 21 | missed |
| All benign baseline | Thursday | Web Attack – XSS | 21 | missed |
| Threshold rules | Friday | Bot | 648 | 0 |
| Isolation Forest | Friday | Bot | 648 | 0 |
| Random Forest (supervised reference upper bound) | Friday | Bot | 648 | missed |
| All benign baseline | Friday | Bot | 648 | missed |
| Threshold rules | Friday | DDoS | 22 | 0 |
| Isolation Forest | Friday | DDoS | 22 | 0 |
| Random Forest (supervised reference upper bound) | Friday | DDoS | 22 | missed |
| All benign baseline | Friday | DDoS | 22 | missed |
| Threshold rules | Friday | PortScan | 27 | 12 |
| Isolation Forest | Friday | PortScan | 27 | 12 |
| Random Forest (supervised reference upper bound) | Friday | PortScan | 27 | 2 |
| All benign baseline | Friday | PortScan | 27 | missed |

## Production Isolation Forest feature coverage

| Production feature | Real flow equivalent |
| --- | --- |
| event_count | flow_count |
| network_event_count | flow_count |
| avg_network_bytes | (total_fwd_bytes + total_bwd_bytes) / flow_count |
| unique_destination_ip_count | distinct_destination_ips |
| login_event_count | unavailable |
| api_event_count | unavailable |
| anomaly_event_count | unavailable |
| failed_login_rate | unavailable |
| failed_auth_count | unavailable |
| requests_per_minute | unavailable |
| avg_api_response_time | unavailable |
| endpoint_error_rate | unavailable |
| api_5xx_rate | unavailable |
| high_risk_network_event_count | unavailable |
| anomaly_rate | unavailable |
| risk_score_moving_average | unavailable |
| high_risk_event_count | unavailable |
| distinct_login_country_count | unavailable |

## Production model applicability

| Contract features | Real equivalents | Contract-v2 metadata features | Evaluated | Reason |
| --- | --- | --- | --- | --- |
| 18 | 4 | not available | False | Missing production telemetry features; filling them would invent values. |
