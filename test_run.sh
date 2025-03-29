# Make sure input.txt and config.json are set up correctly
# Make sure word_counter.sh exists and is executable

# Run using parameters from config.json
python pipeline.py --config config.json

# Run with cleanup
# python pipeline.py --config config.json --cleanup
