# l2-mev
Tracking L2s MEV for spam bot activities & more

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
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
