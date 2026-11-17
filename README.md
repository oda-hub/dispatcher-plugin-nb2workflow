# dispatcher-plugin-nb2workflow

To include the raw output notebook in a progress response, pass
`return_progress=True` and `return_progress_notebook=True` to `run_analysis`.
This requires an nb2workflow backend supporting `include_raw_notebook` on its
trace endpoint. The HTML progress product is unchanged; the base64-encoded
notebook is returned as `output_notebook_content` in each API
`progress_product_list` item, or in the `progress_product_notebook_content`
list for non-API responses. The option defaults to false and does not change
the job identity or notebook inputs.

If the selected notebook already defines a `return_progress_notebook` input,
the parameter remains a notebook input and this option is unavailable.
