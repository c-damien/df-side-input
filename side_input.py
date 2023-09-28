# Copyright 2023 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, 
# software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and limitations under the License. 
# author: cdamien 2023

import apache_beam as beam
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.transforms import window
from apache_beam.utils.timestamp import Timestamp
from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.runners import DataflowRunner
import traceback, sys
import time
import logging
import json

from io import StringIO
from google.cloud import storage

last_timestamp = MAX_TIMESTAMP #to go on indefninitely
first_timestamp = time.time()

logger= logging.getLogger('log')
logger.setLevel(logging.INFO)

# we just return the current side input values for each record
class CrossFilter(beam.DoFn):
 def process(self, left, rights):
    yield rights

# Used to filter messages from the pub/sub topic    
def is_in_scope(t):
  return t['meter_reading'] > 2 and t['meter_reading'] < 5  

options = PipelineOptions(
      flags=[],
       streaming=True,
       project="sfsc-srtt-shared",
       runner='DataflowRunner',
       save_main_session=True,
       region="us-central1",
       requirements_file="requirements.txt",
       max_num_workers=4,
       experiments=["enable_data_sampling"],
       machine_type="n1-highmem-4",
       staging_location="gs://sfsc-df/staging",
       
       temp_location="gs://sfsc-df/temp"
    ) 

topic = "projects/pubsub-public-data/topics/taxirides-realtime"

# Read a file from 
class Load_data_from_gcs(beam.DoFn):
  def process(self, bucketName, filepath):
    from io import StringIO
    from google.cloud import storage
    import json
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucketName)
    blob = bucket.blob(filepath)
    blob = blob.download_as_string()
    blob = blob.decode('utf-8')
    for line in blob.splitlines():
        yield json.loads(line)

# Create pipeline.
# main pipeline
def run():
    import apache_beam as beam
    import time
    
    with beam.Pipeline(DataflowRunner(), options=options) as pipeline: #DataflowRunner #InteractiveRunner(),
        
        # side input
        side_input = (
            pipeline
            | 'PeriodicImpulse' >> PeriodicImpulse(fire_interval=1.0, apply_windowing=True)
            | "MapToFileName" >> beam.ParDo(Load_data_from_gcs("sfsc-df","side_input/items.json"))
            | 'adding si ts' >> beam.Map(lambda x: beam.window.TimestampedValue(x, time.time()))
        )
        
        # displaying the side input
        display = (
            side_input
            | 'd1' >> beam.Map(logging.info)
        )

        # main flow from pub/sub
        main_input = (
            pipeline
            | "Read Topic" >> beam.io.ReadFromPubSub(topic=topic)
            | 'To Json' >> beam.Map(lambda e: json.loads(e.decode('utf-8'))) #parse JSON
            | 'filter' >> beam.Filter(is_in_scope)
            | 'adding  m ts' >> beam.Map(lambda x: beam.window.TimestampedValue(x, time.time()))
            | 'WindowMpInto' >> beam.WindowInto(window.FixedWindows(5))
        )
        
        # combining the side input and main
        result = (
           main_input
            | "ApplyCrossJoin" >> beam.ParDo(CrossFilter(), rights=beam.pvalue.AsList(side_input))
            | 'd2' >> beam.Map(logging.info) # just displaying the side input
        )
        
if __name__ == "__main__":
    run()