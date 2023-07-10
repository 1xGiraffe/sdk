import {
  BuyTransfer,
  Pool,
  PoolBase,
  PoolFee,
  PoolFees,
  PoolPair,
  PoolToken,
  PoolType,
  SellTransfer,
} from '../../types';
import { BigNumber, bnum, ONE, scale, ZERO } from '../../utils/bignumber';
import { toPct } from '../../utils/mapper';

import { LbpMath } from './LbpMath';

export type WeightedPoolPair = PoolPair & {
  weightIn: BigNumber;
  weightOut: BigNumber;
};

export type WeightedPoolToken = PoolToken & {
  weight: BigNumber;
};

export type LbpPoolFees = PoolFees & {
  exchangeFee: PoolFee;
  repayFee: PoolFee;
};

export type LbpPoolBase = PoolBase & {
  repayFeeApply: boolean;
};

export class LbpPool implements Pool {
  type: PoolType;
  address: string;
  tokens: WeightedPoolToken[];
  maxInRatio: number;
  maxOutRatio: number;
  minTradingLimit: number;
  fees: LbpPoolFees;
  repayFeeApply: boolean;

  static fromPool(pool: LbpPoolBase): LbpPool {
    return new LbpPool(
      pool.address,
      pool.tokens as WeightedPoolToken[],
      pool.maxInRatio,
      pool.maxOutRatio,
      pool.minTradingLimit,
      pool.fees as LbpPoolFees,
      pool.repayFeeApply
    );
  }

  constructor(
    address: string,
    tokens: WeightedPoolToken[],
    maxInRation: number,
    maxOutRatio: number,
    minTradeLimit: number,
    fees: LbpPoolFees,
    repayFeeApply: boolean
  ) {
    this.type = PoolType.LBP;
    this.address = address;
    this.tokens = tokens;
    this.maxInRatio = maxInRation;
    this.maxOutRatio = maxOutRatio;
    this.minTradingLimit = minTradeLimit;
    this.fees = fees;
    this.repayFeeApply = repayFeeApply;
  }

  validatePair(_tokenIn: string, _tokenOut: string): boolean {
    return true;
  }

  parsePair(tokenIn: string, tokenOut: string): WeightedPoolPair {
    const tokensMap = new Map(this.tokens.map((token) => [token.id, token]));
    const tokenInMeta = tokensMap.get(tokenIn);
    const tokenOutMeta = tokensMap.get(tokenOut);

    if (tokenInMeta == null) throw new Error('Pool does not contain tokenIn');
    if (tokenOutMeta == null) throw new Error('Pool does not contain tokenOut');

    const balanceIn = bnum(tokenInMeta.balance);
    const balanceOut = bnum(tokenOutMeta.balance);

    return {
      assetIn: tokenIn,
      assetOut: tokenOut,
      decimalsIn: tokenInMeta.decimals,
      decimalsOut: tokenOutMeta.decimals,
      weightIn: tokenInMeta.weight,
      weightOut: tokenOutMeta.weight,
      balanceIn: balanceIn,
      balanceOut: balanceOut,
    } as WeightedPoolPair;
  }

  /**
   * Validate buy transfer
   *
   * a) Accumulated asset is bought (out) from the pool for distributed asset (in) - User(Buyer) bears the fee
   * b) Distributed asset is bought (out) from the pool for accumualted asset (in) - Pool bears the fee
   */
  validateAndBuy(poolPair: WeightedPoolPair, amountOut: BigNumber, dynamicFees: LbpPoolFees): BuyTransfer {
    const feeAsset = this.tokens[0].id;
    if (feeAsset === poolPair.assetOut) {
      const fees: LbpPoolFees = dynamicFees ?? this.fees;
      const fee = this.calculateTradeFee(amountOut, fees);
      const feePct = toPct(this.repayFeeApply ? fees.repayFee : fees.exchangeFee);
      const amountOutPlusFee = amountOut.plus(fee);
      const calculatedIn = this.calculateInGivenOut(poolPair, amountOutPlusFee);
      return {
        amountIn: calculatedIn,
        calculatedIn: calculatedIn,
        amountOut: amountOut,
        feePct: feePct,
      } as BuyTransfer;
    } else {
      const calculatedIn = this.calculateInGivenOut(poolPair, amountOut);
      return { amountIn: calculatedIn, calculatedIn: calculatedIn, amountOut: amountOut, feePct: 0 } as BuyTransfer;
    }
  }

  /**
   * Validate sell transfer
   *
   * a) Accumulated asset is sold (in) to the pool for distributed asset (out) - Pool bears the fee
   * b) Distributed asset is sold (in) to the pool for accumualted asset (out) - User(Seller) bears the fee
   */
  validateAndSell(poolPair: WeightedPoolPair, amountIn: BigNumber, dynamicFees: LbpPoolFees): SellTransfer {
    const feeAsset = this.tokens[0].id;
    if (feeAsset === poolPair.assetIn) {
      const calculatedOut = this.calculateOutGivenIn(poolPair, amountIn);
      return {
        amountIn: amountIn,
        calculatedOut: calculatedOut,
        amountOut: calculatedOut,
        feePct: 0,
      } as SellTransfer;
    } else {
      const calculatedOut = this.calculateOutGivenIn(poolPair, amountIn);
      const fees: LbpPoolFees = dynamicFees ?? this.fees;
      const fee = this.calculateTradeFee(calculatedOut, fees);
      const feePct = toPct(this.repayFeeApply ? fees.repayFee : fees.exchangeFee);
      const amountOut = calculatedOut.minus(fee);
      return {
        amountIn: amountIn,
        calculatedOut: calculatedOut,
        amountOut: amountOut,
        feePct: feePct,
      } as SellTransfer;
    }
  }

  calculateInGivenOut(poolPair: WeightedPoolPair, amountOut: BigNumber): BigNumber {
    const price = LbpMath.calculateInGivenOut(
      poolPair.balanceIn.toString(),
      poolPair.balanceOut.toString(),
      poolPair.weightIn.toString(),
      poolPair.weightOut.toString(),
      amountOut.toFixed(0)
    );
    const priceBN = bnum(price);
    return priceBN.isNegative() ? ZERO : priceBN;
  }

  calculateOutGivenIn(poolPair: WeightedPoolPair, amountIn: BigNumber): BigNumber {
    const price = LbpMath.calculateOutGivenIn(
      poolPair.balanceIn.toString(),
      poolPair.balanceOut.toString(),
      poolPair.weightIn.toString(),
      poolPair.weightOut.toString(),
      amountIn.toFixed(0)
    );
    const priceBN = bnum(price);
    return priceBN.isNegative() ? ZERO : priceBN;
  }

  spotPriceInGivenOut(poolPair: WeightedPoolPair): BigNumber {
    const price = LbpMath.getSpotPrice(
      poolPair.balanceOut.toString(),
      poolPair.balanceIn.toString(),
      poolPair.weightOut.toString(),
      poolPair.weightIn.toString(),
      scale(ONE, poolPair.decimalsOut).toString()
    );
    return bnum(price);
  }

  spotPriceOutGivenIn(poolPair: WeightedPoolPair): BigNumber {
    const price = LbpMath.getSpotPrice(
      poolPair.balanceIn.toString(),
      poolPair.balanceOut.toString(),
      poolPair.weightIn.toString(),
      poolPair.weightOut.toString(),
      scale(ONE, poolPair.decimalsIn).toString()
    );
    return bnum(price);
  }

  calculateTradeFee(amount: BigNumber, fees: LbpPoolFees): BigNumber {
    const fee = LbpMath.calculatePoolTradeFee(
      amount.toString(),
      this.repayFeeApply ? fees.repayFee[0] : fees.exchangeFee[0],
      this.repayFeeApply ? fees.repayFee[1] : fees.exchangeFee[1]
    );
    return bnum(fee);
  }
}
