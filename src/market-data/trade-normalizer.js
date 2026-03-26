const SOL_MINT = 'So11111111111111111111111111111111111111112';
const STABLE_MINTS = new Set([
  'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
  'Es9vMFrzaCERmJfrF4H2FYDmuE1b9YuqbWNoXDpJj7n'
]);

function toFiniteNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

function getPoolAccounts(poolAddress) {
  return new Set([poolAddress].filter(Boolean));
}

function pickTokenAmount(transfer) {
  return Math.abs(
    toFiniteNumber(
      transfer?.tokenAmount ??
      transfer?.rawTokenAmount?.tokenAmount ??
      transfer?.rawTokenAmount?.uiAmount ??
      transfer?.amount
    )
  );
}

function extractBaseTransfer(tx, tokenCa, poolAddress) {
  const poolAccounts = getPoolAccounts(poolAddress);
  const transfers = tx?.tokenTransfers || [];

  const exact = transfers.find((transfer) => {
    if (transfer?.mint !== tokenCa) return false;
    return poolAccounts.has(transfer?.fromUserAccount) || poolAccounts.has(transfer?.toUserAccount);
  });
  if (exact) return exact;

  return transfers.find((transfer) => transfer?.mint === tokenCa) || null;
}

function extractQuoteAmount(tx, tokenCa, poolAddress) {
  const poolAccounts = getPoolAccounts(poolAddress);
  const tokenTransfers = tx?.tokenTransfers || [];
  const nativeTransfers = tx?.nativeTransfers || [];

  const stableTransfer = tokenTransfers.find((transfer) => {
    if (!transfer?.mint || transfer.mint === tokenCa) return false;
    if (!STABLE_MINTS.has(transfer.mint)) return false;
    return poolAccounts.has(transfer?.fromUserAccount) || poolAccounts.has(transfer?.toUserAccount);
  }) || tokenTransfers.find((transfer) => transfer?.mint && transfer.mint !== tokenCa && STABLE_MINTS.has(transfer.mint));

  if (stableTransfer) {
    return { amount: pickTokenAmount(stableTransfer), mint: stableTransfer.mint };
  }

  const nativeTransfer = nativeTransfers.find((transfer) => poolAccounts.has(transfer?.fromUserAccount) || poolAccounts.has(transfer?.toUserAccount)) || nativeTransfers[0];
  if (nativeTransfer) {
    return { amount: Math.abs(toFiniteNumber(nativeTransfer.amount)) / 1e9, mint: SOL_MINT };
  }

  const nonBaseTransfer = tokenTransfers.find((transfer) => transfer?.mint && transfer.mint !== tokenCa);
  if (nonBaseTransfer) {
    return { amount: pickTokenAmount(nonBaseTransfer), mint: nonBaseTransfer.mint };
  }

  return { amount: 0, mint: null };
}

function deriveSide(baseTransfer, poolAddress) {
  if (!baseTransfer || !poolAddress) return null;
  if (baseTransfer.toUserAccount === poolAddress) return 'sell';
  if (baseTransfer.fromUserAccount === poolAddress) return 'buy';
  return null;
}

export class TradeNormalizer {
  normalizeTransaction(tx, { tokenCa, poolAddress }) {
    if (!tx || !tokenCa || !poolAddress) {
      return null;
    }

    const signature = tx.signature || tx.transactionError?.signature || null;
    const blockTime = Number(tx.timestamp || tx.blockTime || 0);
    const slot = Number(tx.slot || 0);
    const type = String(tx.type || '').toUpperCase();

    if (!signature || !blockTime || tx.transactionError || tx.meta?.err) {
      return null;
    }

    const baseTransfer = extractBaseTransfer(tx, tokenCa, poolAddress);
    const baseAmount = pickTokenAmount(baseTransfer);
    if (!baseAmount) {
      return null;
    }

    const { amount: quoteAmount } = extractQuoteAmount(tx, tokenCa, poolAddress);
    const volume = Math.abs(quoteAmount);
    const price = baseAmount > 0 ? volume / baseAmount : 0;
    if (!Number.isFinite(price) || price <= 0) {
      return null;
    }

    const isSwapLike = type === 'SWAP' || (tx.events && tx.events.swap) || (tx.tokenTransfers || []).length >= 2;
    if (!isSwapLike) {
      return null;
    }

    return {
      signature,
      slot,
      blockTime,
      tokenCa,
      poolAddress,
      price,
      baseAmount,
      quoteAmount: volume,
      volume,
      side: deriveSide(baseTransfer, poolAddress),
      source: 'helius'
    };
  }

  normalizeTransactions(transactions = [], context = {}) {
    return transactions
      .map((tx) => this.normalizeTransaction(tx, context))
      .filter(Boolean);
  }
}

export default TradeNormalizer;
