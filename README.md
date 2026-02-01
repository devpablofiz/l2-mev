# l2-mev
Tracking L2s MEV for spam bot activities & more

## Setup

1. Install dependencies:
   ```bash
   python -m pip install -r requirements.txt
   ```

2. Configure `.env`:
   Update the `DB_PASSWORD` and other database settings in the `.env` file.

3. Run the transaction loader:
   ```bash
   python load_transactions.py <start_block> <end_block>
   ```

## Example
To load blocks from 41020000 to 41020010:
```bash
python load_transactions.py 41020000 41020010
```

## MEV Tagging GUI

To help identify bots and spam, use the built-in tagging tool:

1. Start the Streamlit app:
   ```bash
   python -m streamlit run tagger_gui.py
   ```

2. **Tag Methods**: Identify top untagged calldata patterns and assign them tags (e.g., "Uniswap Swap").
3. **Auto-Tag Senders**: Click the "Run Auto-Tagging" button to automatically label the top 100 senders if they've used any of your tagged methods in their last 20 transactions.
