python 0131_scrapeSEC.py \
	--project encoded-site-265218 \
	--runner DataflowRunner \
	--staging_location gs://demo-temp-bucket/staging \
	--temp_location gs://demo-temp-bucket/temp \
	--output gs://demo-temp-bucket/output2
