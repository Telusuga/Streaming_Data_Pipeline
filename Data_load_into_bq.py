!pip install apache-beam[gcp]

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

beam_options=PipelineOptions(streaming=True,save_main_session=True)

with beam.Pipeline(options=beam_options) as P:
    test=(
        P
        | 'reading data from Pubsub Topic' >> beam.io.ReadFromPubSub('projects/dataflow-test-414808/topics/final')
        | 'decoding the byte record to string' >> beam.Map(lambda x: x.decode('utf-8'))
        | 'testing the value' >> beam.Map(eval)
        | 'loading the data into bigquery table' >>beam.io.Write( beam.io.WriteToBigQuery(
            table='pipeline',
            project='dataflow-test-414808',
            dataset='df_load',
            schema='id:INTEGER,Name:STRING,CITY:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        ))                                                   
    )
