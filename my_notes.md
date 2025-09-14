# my understanding of run_multiprocess()

                ┌───────────────────────────────────────────────┐
                │               run_multiprocess                │
                └───────────────────────────────────────────────┘
                                        │
                                        ▼
                ┌───────────────────────────────────────────────┐
                │ 1. Read data.json → DataFrame                 │
                └───────────────────────────────────────────────┘
                                        │
                                        ▼
                ┌───────────────────────────────────────────────┐
                │ 2. Group by `pool_over` → List of DataFrames  │
                └───────────────────────────────────────────────┘
                                        │
                                        ▼
                ┌───────────────────────────────────────────────┐
                │ 3. Split into N chunks (batches)              │
                └───────────────────────────────────────────────┘
                                        │
                                        ▼
        ┌───────────────────────────────────────────────────────────────┐
        │                 4. Multiprocessing Manager                    │
        │  ┌─────────────────────────────┐   ┌────────────────────────┐ │
        │  │ Process 1                   │   │ Process 2              │ │
        │  │ estimate_batch()            │   │ estimate_batch()       │ │
        │  │                             │   │                        │ │
        │  │  ┌──────────────────────┐   │   │  ┌──────────────────┐  │ │
        │  │  │ Loop over chunk      │   │   │  │ Loop over chunk  │  │ │
        │  │  │  retailer_data[i]    │   │   │  │ retailer_data[i] │  │ │
        │  │  └──────────┬───────────┘   │   │  └─────────┬────────┘  │ │
        │  │             │               │   │            │           │ │
        │  │             ▼               │   │            ▼           │ │
        │  │    ┌─────────────────────┐  │   │   ┌─────────────────┐  │ │
        │  │    │ Call process(df,    │  │   │   │ Call process(df,│  │ │
        │  │    │ method)             │  │   │   │ method)         │  │ │
        │  │    └─────────────────────┘  │   │   └─────────────────┘  │ │
        │  │             │               │   │            │           │ │
        │  │   Collect next_df, failed   │   │ Collect next_df, failed│ │
        │  │   Append to out_dfs         │   │ Append to out_dfs      │ │
        │  └─────────────┬───────────────┘   └───────────┬────────────┘ │
        │                ▼                               ▼              │
        │       ┌───────────────────────┐     ┌──────────────────────┐  │
        │       │ Concatenate out_dfs   │     │ Concatenate out_dfs  │  │
        │       │ Append to outputs     │     │ Append to outputs    │  │
        │       └───────────────────────┘     └──────────────────────┘  │
        └───────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
                ┌───────────────────────────────────────────────┐
                │ 5. Combine all outputs → Final DataFrame      │
                │ Save to output.json                           │
                └───────────────────────────────────────────────┘







### results after implementing json flattening in beam pipeline.

{'item_id': 'i1', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 65.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.14}
{'item_id': 'i1', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 78.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.25, 'effective_discount': 0.19}
{'item_id': 'i1', 'tactic': 'x_for_y', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 82.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.18}
{'item_id': 'i1', 'tactic': 'x_for_y', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 90.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.3, 'effective_discount': 0.21}
{'item_id': 'i2', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 90.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.3, 'effective_discount': 0.29}
{'item_id': 'i2', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 70.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.17}
{'item_id': 'i2', 'tactic': 'x_for_y', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 1.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.25, 'effective_discount': 0.22}
{'item_id': 'i3', 'tactic': 'x_for_y', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 80.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.3, 'effective_discount': 0.26}
{'item_id': 'i3', 'tactic': 'new_amount', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 50.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.1, 'effective_discount': 0.04}
{'item_id': 'i3', 'tactic': 'new_amount', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 60.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.1}
{'item_id': 'i3', 'tactic': 'x_for_y', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 96.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.25, 'effective_discount': 0.24}
{'item_id': 'i4', 'tactic': 'x_for_y', 'lowest_category': 'l4', 'highest_category': 'h2', 'promo_quantity': 45.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.1, 'effective_discount': 0.02}
{'item_id': 'i4', 'tactic': 'new_amount', 'lowest_category': 'l4', 'highest_category': 'h2', 'promo_quantity': 55.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.15, 'effective_discount': 0.04}


### results after normalization
{'item_id': 'i1', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 65.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.14}
{'item_id': 'i1', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 78.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.25, 'effective_discount': 0.19}
{'item_id': 'i1', 'tactic': 'x_for_y', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 82.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.18}
{'item_id': 'i1', 'tactic': 'x_for_y', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 90.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.3, 'effective_discount': 0.21}
{'item_id': 'i2', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 90.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.3, 'effective_discount': 0.29}
{'item_id': 'i2', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 70.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.17}
{'item_id': 'i2', 'tactic': 'x_for_y', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 1.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.25, 'effective_discount': 0.22}
{'item_id': 'i3', 'tactic': 'x_for_y', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 80.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.3, 'effective_discount': 0.26}
{'item_id': 'i3', 'tactic': 'new_amount', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 50.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.1, 'effective_discount': 0.04}
{'item_id': 'i3', 'tactic': 'new_amount', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 60.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.1}
{'item_id': 'i3', 'tactic': 'x_for_y', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 96.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.25, 'effective_discount': 0.24}
{'item_id': 'i4', 'tactic': 'x_for_y', 'lowest_category': 'l4', 'highest_category': 'h2', 'promo_quantity': 45.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.1, 'effective_discount': 0.02}
{'item_id': 'i4', 'tactic': 'new_amount', 'lowest_category': 'l4', 'highest_category': 'h2', 'promo_quantity': 55.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.15, 'effective_discount': 0.04}

### results after keying transformation


('i1', {'item_id': 'i1', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 65.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.14})
('i1', {'item_id': 'i1', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 78.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.25, 'effective_discount': 0.19})
('i1', {'item_id': 'i1', 'tactic': 'x_for_y', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 82.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.18})
('i1', {'item_id': 'i1', 'tactic': 'x_for_y', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 90.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.3, 'effective_discount': 0.21})
('i2', {'item_id': 'i2', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 90.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.3, 'effective_discount': 0.29})
('i2', {'item_id': 'i2', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 70.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.17})
('i2', {'item_id': 'i2', 'tactic': 'x_for_y', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 1.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.25, 'effective_discount': 0.22})
('i3', {'item_id': 'i3', 'tactic': 'x_for_y', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 80.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.3, 'effective_discount': 0.26})
('i3', {'item_id': 'i3', 'tactic': 'new_amount', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 50.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.1, 'effective_discount': 0.04})
('i3', {'item_id': 'i3', 'tactic': 'new_amount', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 60.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.1})
('i3', {'item_id': 'i3', 'tactic': 'x_for_y', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 96.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.25, 'effective_discount': 0.24})
('i4', {'item_id': 'i4', 'tactic': 'x_for_y', 'lowest_category': 'l4', 'highest_category': 'h2', 'promo_quantity': 45.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.1, 'effective_discount': 0.02})
('i4', {'item_id': 'i4', 'tactic': 'new_amount', 'lowest_category': 'l4', 'highest_category': 'h2', 'promo_quantity': 55.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.15, 'effective_discount': 0.04})


### results after aggregating

('i3', [{'item_id': 'i3', 'tactic': 'x_for_y', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 80.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.3, 'effective_discount': 0.26}, {'item_id': 'i3', 'tactic': 'new_amount', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 50.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.1, 'effective_discount': 0.04}, {'item_id': 'i3', 'tactic': 'new_amount', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 60.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.1}, {'item_id': 'i3', 'tactic': 'x_for_y', 'lowest_category': 'l2', 'highest_category': 'h1', 'promo_quantity': 96.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.25, 'effective_discount': 0.24}])
('i1', [{'item_id': 'i1', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 65.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.14}, {'item_id': 'i1', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 78.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.25, 'effective_discount': 0.19}, {'item_id': 'i1', 'tactic': 'x_for_y', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 82.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.18}, {'item_id': 'i1', 'tactic': 'x_for_y', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 90.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.3, 'effective_discount': 0.21}])
('i4', [{'item_id': 'i4', 'tactic': 'x_for_y', 'lowest_category': 'l4', 'highest_category': 'h2', 'promo_quantity': 45.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.1, 'effective_discount': 0.02}, {'item_id': 'i4', 'tactic': 'new_amount', 'lowest_category': 'l4', 'highest_category': 'h2', 'promo_quantity': 55.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.15, 'effective_discount': 0.04}])
('i2', [{'item_id': 'i2', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 90.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.3, 'effective_discount': 0.29}, {'item_id': 'i2', 'tactic': 'new_amount', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 70.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.2, 'effective_discount': 0.17}, {'item_id': 'i2', 'tactic': 'x_for_y', 'lowest_category': 'l1', 'highest_category': 'h1', 'promo_quantity': 1.0, 'sales_quantity': 100.0, 'theoretical_discount': 0.25, 'effective_discount': 0.22}])

### results after process function call

{'item_id': 'i3', 'alpha': 1.0, 'beta': 0.0}
{'item_id': 'i2', 'alpha': 1.0, 'beta': 0.0}
{'item_id': 'i4', 'alpha': 1.0, 'beta': 0.0}
{'item_id': 'i1', 'alpha': 1.0, 'beta': 0.0}

where as expected result is : {"item_id":{"0":"i3","1":"i4","2":"i1","3":"i2"},"alpha":{"0":1.0,"1":1.0,"2":1.0,"3":1.0},"beta":{"0":0.0,"1":0.0,"2":0.0,"3":0.0}}