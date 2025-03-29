# Make sure input.txt and config.json are set up correctly
# Make sure word_counter.sh exists and is executable

# Run using parameters from config.json
python -m workflow.pipeline --config config/config.json


# Run with cleanup
# python -m workflow.pipeline --config config/config.json --cleanup
