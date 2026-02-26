/**
 * BSC Chain Snapshot Module
 *
 * Responsibilities:
 * - Honeypot检测
 * - Tax检测 (buy/sell/mutable)
 * - Owner分析 (Renounced/MultiSig/TimeLock/EOA)
 * - Dangerous Functions检测
 * - LP Lock验证
 * - 流动性/交易量获取
 * - Top10持仓分析
 * - 卖出限制检测
 */

import axios from 'axios';
import { ethers } from 'ethers';

export class BSCSnapshotService {
  constructor(config) {
    this.config = config;
    this.provider = new ethers.JsonRpcProvider(
      process.env.BSC_RPC_URL || 'https://bsc-dataseed.binance.org'
    );

    // Known safe owner addresses (burn/timelock contracts)
    this.safeOwnerAddresses = new Set([
      '0x000000000000000000000000000000000000dead', // Burn address
      '0x0000000000000000000000000000000000000000', // Zero address
    ]);

    // Known MultiSig contract code signatures
    this.multiSigSignatures = [
      '0x7065cb48', // Gnosis Safe
      '0xa0b86991', // Common MultiSig pattern
    ];

    // Dangerous function selectors
    this.dangerousFunctions = {
      'setTax': '0x',  // Various tax setting functions
      'setFee': '0x',
      'setMarketingFee': '0x',
      'blacklist': '0x',
      'addToBlacklist': '0x',
      'enableTrading': '0x',
      'setTrading': '0x',
      'setCooldown': '0x',
      'setMaxTx': '0x',
      'setMaxWallet': '0x'
    };
  }

  /**
   * Main entry: Get complete snapshot for a BSC token
   */
  async getSnapshot(tokenCA, plannedPosition = null) {
    console.log(`📸 [BSC] Getting snapshot for ${tokenCA}`);

    try {
      const [
        honeypotData,
        taxData,
        ownerData,
        dangerousData,
        lpLockData,
        liquidityData,
        top10Data,
        sellConstraints
      ] = await Promise.allSettled([
        this.checkHoneypot(tokenCA),
        this.analyzeTax(tokenCA),
        this.analyzeOwner(tokenCA),
        this.checkDangerousFunctions(tokenCA),
        this.verifyLPLock(tokenCA),
        this.getLiquidity(tokenCA),
        this.getTop10Analysis(tokenCA),
        this.checkSellConstraints(tokenCA)
      ]);

      return {
        // Basic info
        chain: 'BSC',
        token_ca: tokenCA,

        // Honeypot
        honeypot: this.unwrap(honeypotData)?.status || 'Unknown',
        honeypot_reason: this.unwrap(honeypotData)?.reason,

        // Tax
        tax_buy: this.unwrap(taxData)?.buy_tax,
        tax_sell: this.unwrap(taxData)?.sell_tax,
        tax_mutable: this.unwrap(taxData)?.is_mutable,
        tax_max_cap: this.unwrap(taxData)?.max_cap,

        // Owner
        owner_type: this.unwrap(ownerData)?.type || 'Unknown',
        owner_address: this.unwrap(ownerData)?.address,
        owner_safe: this.unwrap(ownerData)?.is_safe,

        // Dangerous functions
        dangerous_functions: this.unwrap(dangerousData)?.functions || [],
        has_blacklist: this.unwrap(dangerousData)?.has_blacklist || false,
        has_trading_control: this.unwrap(dangerousData)?.has_trading_control || false,

        // LP Lock
        lp_lock: this.unwrap(lpLockData)?.locked || false,
        lp_lock_platform: this.unwrap(lpLockData)?.platform,
        lp_lock_duration: this.unwrap(lpLockData)?.duration_days,
        lp_lock_percentage: this.unwrap(lpLockData)?.percentage,
        lp_lock_proof: this.unwrap(lpLockData)?.proof,

        // Liquidity & Volume
        liquidity: this.unwrap(liquidityData)?.liquidity_bnb,
        liquidity_unit: 'BNB',
        liquidity_usd: this.unwrap(liquidityData)?.liquidity_usd,
        vol_24h_usd: this.unwrap(liquidityData)?.volume_24h,

        // Top10
        top10_percent: this.unwrap(top10Data)?.top10_percent,
        holder_count: this.unwrap(top10Data)?.holder_count,

        // Sell constraints
        sell_constraints_flag: this.unwrap(sellConstraints)?.has_constraints || false,
        sell_constraints: this.unwrap(sellConstraints)?.constraints || [],

        // Metadata
        snapshot_time: Date.now(),
        data_source: 'GoPlus + DexScreener + BscScan'
      };
    } catch (error) {
      console.error(`❌ [BSC] Snapshot error for ${tokenCA}:`, error.message);
      return this.getUnknownSnapshot();
    }
  }

  /**
   * Check if token is honeypot using GoPlus API + Gas Limit Check
   */
  async checkHoneypot(tokenCA) {
    try {
      const url = `https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses=${tokenCA}`;
      const response = await axios.get(url, { timeout: 10000 });

      const data = response.data?.result?.[tokenCA.toLowerCase()];

      if (!data) {
        return { status: 'Unknown' };
      }

      // GoPlus provides is_honeypot flag
      const isHoneypot = data.is_honeypot === '1';
      const canSell = data.can_take_back_ownership !== '1' && data.cannot_sell_all !== '1';

      // Gas Limit Check (防Honeypot)
      const gasLimitCheck = await this.checkGasLimit(tokenCA);
      const gasLimitExceeded = gasLimitCheck.exceeded;

      let status, reason;

      if (isHoneypot || !canSell || gasLimitExceeded) {
        status = 'Fail';
        if (isHoneypot) {
          reason = 'Detected as honeypot';
        } else if (!canSell) {
          reason = 'Cannot sell detected';
        } else if (gasLimitExceeded) {
          reason = `Gas limit exceeded: ${gasLimitCheck.estimatedGas} > ${gasLimitCheck.maxGasLimit}`;
        }
      } else {
        status = 'Pass';
        reason = null;
      }

      return {
        status,
        reason,
        raw_data: {
          is_honeypot: data.is_honeypot,
          can_sell: canSell,
          buy_tax: data.buy_tax,
          sell_tax: data.sell_tax,
          gas_limit_check: gasLimitCheck
        }
      };
    } catch (error) {
      console.error('GoPlus honeypot check error:', error.message);
      return { status: 'Unknown' };
    }
  }

  /**
   * Check gas limit for sell transaction (防Honeypot)
   * 如果 estimateGas > 1,000,000 则可能是 Honeypot
   */
  async checkGasLimit(tokenCA) {
    try {
      // ERC20 transfer function signature
      const transferInterface = new ethers.Interface([
        'function transfer(address to, uint256 amount) returns (bool)'
      ]);

      // Create dummy transfer call data (0.001 token to dead address)
      const transferData = transferInterface.encodeFunctionData('transfer', [
        '0x000000000000000000000000000000000000dead',  // dead address
        ethers.parseUnits('0.001', 18)  // small amount
      ]);

      // Estimate gas for the transfer (simulates sell)
      const gasEstimate = await this.provider.estimateGas({
        to: tokenCA,
        data: transferData,
        from: '0x0000000000000000000000000000000000000001'  // dummy sender
      });

      const estimatedGas = Number(gasEstimate);
      const maxGasLimit = 1000000;  // 1M gas threshold
      const exceeded = estimatedGas > maxGasLimit;

      if (exceeded) {
        console.log(`   ⚠️  Gas limit check FAIL: ${estimatedGas} > ${maxGasLimit}`);
      }

      return {
        estimatedGas,
        maxGasLimit,
        exceeded,
        safe: !exceeded
      };
    } catch (error) {
      // If estimation fails, it might also indicate honeypot
      console.log(`   ⚠️  Gas estimation failed: ${error.message}`);
      return {
        estimatedGas: null,
        maxGasLimit: 1000000,
        exceeded: true,  // Treat estimation failure as honeypot indicator
        safe: false,
        error: error.message
      };
    }
  }

  /**
   * Analyze tax (buy/sell/mutable)
   */
  async analyzeTax(tokenCA) {
    try {
      // Use GoPlus for tax detection
      const url = `https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses=${tokenCA}`;
      const response = await axios.get(url, { timeout: 10000 });

      const data = response.data?.result?.[tokenCA.toLowerCase()];

      if (!data) {
        return null;
      }

      const buyTax = parseFloat(data.buy_tax) * 100 || 0; // Convert to percentage
      const sellTax = parseFloat(data.sell_tax) * 100 || 0;
      const isMutable = data.slippage_modifiable === '1' || data.can_take_back_ownership === '1';

      // Check for max cap (requires contract analysis)
      const maxCap = await this.checkTaxCap(tokenCA);

      return {
        buy_tax: buyTax,
        sell_tax: sellTax,
        is_mutable: isMutable,
        max_cap: maxCap
      };
    } catch (error) {
      console.error('Tax analysis error:', error.message);
      return null;
    }
  }

  /**
   * Check if tax has a hardcoded cap
   */
  async checkTaxCap(tokenCA) {
    try {
      // Get contract source code from BscScan
      if (!process.env.BSCSCAN_API_KEY) {
        return null; // Cannot verify without API key
      }

      const url = `https://api.bscscan.com/api?module=contract&action=getsourcecode&address=${tokenCA}&apikey=${process.env.BSCSCAN_API_KEY}`;
      const response = await axios.get(url, { timeout: 10000 });

      const sourceCode = response.data?.result?.[0]?.SourceCode;

      if (!sourceCode) {
        return null;
      }

      // Look for tax cap patterns in source code
      const capPatterns = [
        /maxFee\s*=\s*(\d+)/,
        /MAX_FEE\s*=\s*(\d+)/,
        /feeMax\s*=\s*(\d+)/,
        /require\s*\(\s*[^,]+\s*<=\s*(\d+)/  // require(fee <= X)
      ];

      for (const pattern of capPatterns) {
        const match = sourceCode.match(pattern);
        if (match) {
          const cap = parseInt(match[1]);
          return cap;
        }
      }

      return null; // No cap found
    } catch (error) {
      console.error('Tax cap check error:', error.message);
      return null;
    }
  }

  /**
   * Analyze owner type and safety
   */
  async analyzeOwner(tokenCA) {
    try {
      // Get contract instance
      const code = await this.provider.getCode(tokenCA);

      // Try to get owner from contract (common patterns)
      const ownerAddress = await this.getOwnerAddress(tokenCA);

      if (!ownerAddress) {
        return { type: 'Unknown', address: null, is_safe: false };
      }

      // Check if owner is renounced (burn/zero address)
      if (this.safeOwnerAddresses.has(ownerAddress.toLowerCase())) {
        return {
          type: 'Renounced',
          address: ownerAddress,
          is_safe: true
        };
      }

      // Check if owner is a contract (MultiSig/TimeLock)
      const ownerCode = await this.provider.getCode(ownerAddress);

      if (ownerCode && ownerCode !== '0x') {
        // It's a contract - determine type
        const type = await this.identifyContractType(ownerAddress, ownerCode);
        return {
          type,
          address: ownerAddress,
          is_safe: type === 'MultiSig' || type === 'TimeLock'
        };
      }

      // It's an EOA (regular wallet)
      return {
        type: 'EOA',
        address: ownerAddress,
        is_safe: false
      };
    } catch (error) {
      console.error('Owner analysis error:', error.message);
      return { type: 'Unknown', address: null, is_safe: false };
    }
  }

  /**
   * Get owner address from contract
   */
  async getOwnerAddress(tokenCA) {
    try {
      const contract = new ethers.Contract(
        tokenCA,
        [
          'function owner() view returns (address)',
          'function getOwner() view returns (address)'
        ],
        this.provider
      );

      try {
        return await contract.owner();
      } catch (e1) {
        try {
          return await contract.getOwner();
        } catch (e2) {
          return null;
        }
      }
    } catch (error) {
      return null;
    }
  }

  /**
   * Identify contract type (MultiSig/TimeLock/Other)
   */
  async identifyContractType(address, code) {
    // Check for known MultiSig signatures
    for (const sig of this.multiSigSignatures) {
      if (code.includes(sig.slice(2))) {
        return 'MultiSig';
      }
    }

    // Check for TimeLock patterns (delay/queue functions)
    if (code.includes('delay') || code.includes('executeTransaction') || code.includes('queueTransaction')) {
      return 'TimeLock';
    }

    return 'Contract'; // Unknown contract type
  }

  /**
   * Check for dangerous functions in contract
   */
  async checkDangerousFunctions(tokenCA) {
    try {
      if (!process.env.BSCSCAN_API_KEY) {
        return { functions: [], has_blacklist: false, has_trading_control: false };
      }

      // Get contract ABI
      const url = `https://api.bscscan.com/api?module=contract&action=getabi&address=${tokenCA}&apikey=${process.env.BSCSCAN_API_KEY}`;
      const response = await axios.get(url, { timeout: 10000 });

      const abiString = response.data?.result;

      // Check for errors or unverified contract
      if (!abiString ||
          abiString === 'Contract source code not verified' ||
          typeof abiString !== 'string' ||
          abiString.startsWith('You are') ||  // API error message
          !abiString.startsWith('[')) {        // Valid ABI should start with [
        return { functions: [], has_blacklist: null, has_trading_control: null };
      }

      let abi;
      try {
        abi = JSON.parse(abiString);
      } catch (parseError) {
        // Invalid JSON response
        return { functions: [], has_blacklist: null, has_trading_control: null };
      }

      // Check for dangerous function names
      const dangerousFuncs = [];
      let hasBlacklist = false;
      let hasTradingControl = false;

      for (const item of abi) {
        if (item.type !== 'function') continue;

        const funcName = item.name.toLowerCase();

        // Check for blacklist functions
        if (funcName.includes('blacklist') || funcName.includes('block')) {
          hasBlacklist = true;
          dangerousFuncs.push(item.name);
        }

        // Check for trading control
        if (funcName.includes('trading') && funcName.includes('enable') || funcName.includes('pause')) {
          hasTradingControl = true;
          dangerousFuncs.push(item.name);
        }

        // Check for tax/fee modification
        if (funcName.includes('settax') || funcName.includes('setfee') || funcName.includes('setmarketing')) {
          dangerousFuncs.push(item.name);
        }

        // Check for max transaction limits
        if (funcName.includes('setmax') || funcName.includes('setlimit')) {
          dangerousFuncs.push(item.name);
        }

        // Check for cooldown
        if (funcName.includes('cooldown')) {
          dangerousFuncs.push(item.name);
        }
      }

      return {
        functions: dangerousFuncs,
        has_blacklist: hasBlacklist,
        has_trading_control: hasTradingControl
      };
    } catch (error) {
      console.error('Dangerous functions check error:', error.message);
      return { functions: [], has_blacklist: null, has_trading_control: null };
    }
  }

  /**
   * Verify LP lock on Pink/Unicrypt/DxSale
   */
  async verifyLPLock(tokenCA) {
    try {
      // Check common lock platforms
      const locks = await Promise.allSettled([
        this.checkPinkSaleLock(tokenCA),
        this.checkUnicryptLock(tokenCA),
        this.checkDxSaleLock(tokenCA)
      ]);

      // Find the best lock (longest duration, highest %)
      const validLocks = locks
        .filter(l => l.status === 'fulfilled' && l.value.locked)
        .map(l => l.value);

      if (validLocks.length === 0) {
        return {
          locked: false,
          platform: null,
          duration_days: 0,
          percentage: 0,
          proof: null
        };
      }

      // Return the lock with longest duration
      const bestLock = validLocks.reduce((max, lock) =>
        lock.duration_days > max.duration_days ? lock : max
      );

      return bestLock;
    } catch (error) {
      console.error('LP lock verification error:', error.message);
      return {
        locked: null,
        platform: null,
        duration_days: null,
        percentage: null,
        proof: null
      };
    }
  }

  /**
   * Check PinkSale lock
   */
  async checkPinkSaleLock(tokenCA) {
    // PinkSale lock checker API (if available)
    // For now, return not locked as we need their API
    return { locked: false };
  }

  /**
   * Check Unicrypt lock
   */
  async checkUnicryptLock(tokenCA) {
    // Unicrypt lock checker
    // Need to query their contract or API
    return { locked: false };
  }

  /**
   * Check DxSale lock
   */
  async checkDxSaleLock(tokenCA) {
    // DxSale lock checker
    return { locked: false };
  }

  /**
   * Get liquidity and volume from DexScreener
   */
  async getLiquidity(tokenCA) {
    try {
      const url = `https://api.dexscreener.com/latest/dex/tokens/${tokenCA}`;
      const response = await axios.get(url, { timeout: 10000 });

      if (!response.data.pairs || response.data.pairs.length === 0) {
        return null;
      }

      // Get largest liquidity pair
      const pair = response.data.pairs.reduce((max, p) =>
        (p.liquidity?.usd || 0) > (max.liquidity?.usd || 0) ? p : max
      );

      const liquidityUSD = pair.liquidity?.usd || 0;
      const volume24h = parseFloat(pair.volume?.h24 || 0);

      // Estimate BNB liquidity
      const bnbPrice = await this.getBNBPrice();
      const liquidityBNB = liquidityUSD / 2 / bnbPrice;

      return {
        liquidity_bnb: liquidityBNB,
        liquidity_usd: liquidityUSD,
        volume_24h: volume24h
      };
    } catch (error) {
      console.error('Liquidity fetch error:', error.message);
      return null;
    }
  }

  /**
   * Get BNB price in USD
   */
  async getBNBPrice() {
    try {
      const response = await axios.get(
        'https://api.coingecko.com/api/v3/simple/price?ids=binancecoin&vs_currencies=usd',
        { timeout: 5000 }
      );
      return response.data.binancecoin.usd;
    } catch (error) {
      return 300; // Fallback approximate price
    }
  }

  /**
   * Analyze Top10 holders
   */
  async getTop10Analysis(tokenCA) {
    try {
      // Use GoPlus for holder distribution
      const url = `https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses=${tokenCA}`;
      const response = await axios.get(url, { timeout: 10000 });

      const data = response.data?.result?.[tokenCA.toLowerCase()];

      if (!data || !data.holder_count) {
        return { top10_percent: null, holder_count: null };
      }

      // GoPlus provides top 10 holder percentage
      const holderCount = parseInt(data.holder_count) || 0;
      const top10 = parseFloat(data.lp_holder_percent) || null; // This might not be Top10

      // If GoPlus doesn't provide, we need to query holders directly
      // This requires more advanced API or RPC calls

      return {
        top10_percent: top10,
        holder_count: holderCount
      };
    } catch (error) {
      console.error('Top10 analysis error:', error.message);
      return { top10_percent: null, holder_count: null };
    }
  }

  /**
   * Check for sell constraints (maxSell/cooldown/etc.)
   */
  async checkSellConstraints(tokenCA) {
    try {
      const url = `https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses=${tokenCA}`;
      const response = await axios.get(url, { timeout: 10000 });

      const data = response.data?.result?.[tokenCA.toLowerCase()];

      if (!data) {
        return { has_constraints: null, constraints: [] };
      }

      const constraints = [];

      // Check for various constraints from GoPlus
      if (data.trading_cooldown === '1') {
        constraints.push('Trading cooldown enabled');
      }

      if (data.cannot_sell_all === '1') {
        constraints.push('Cannot sell all tokens');
      }

      if (data.transfer_pausable === '1') {
        constraints.push('Transfer can be paused');
      }

      return {
        has_constraints: constraints.length > 0,
        constraints
      };
    } catch (error) {
      console.error('Sell constraints check error:', error.message);
      return { has_constraints: null, constraints: [] };
    }
  }

  /**
   * Helper: Unwrap Promise.allSettled result
   */
  unwrap(settledResult) {
    return settledResult.status === 'fulfilled' ? settledResult.value : null;
  }

  /**
   * Return Unknown snapshot when all checks fail
   */
  getUnknownSnapshot() {
    return {
      honeypot: 'Unknown',
      tax_buy: null,
      tax_sell: null,
      tax_mutable: null,
      owner_type: 'Unknown',
      dangerous_functions: [],
      lp_lock: null,
      liquidity: null,
      liquidity_unit: 'BNB',
      vol_24h_usd: null,
      top10_percent: null,
      sell_constraints_flag: null,
      snapshot_time: Date.now(),
      data_source: 'Failed'
    };
  }
}

export default BSCSnapshotService;
