import {
  buildRoute,
  PoolService,
  PoolType,
  TradeRouter,
} from '@galacticcouncil/sdk';
import {
  AnyChain,
  AssetAmount,
  ChainEcosystem,
  ConfigService,
  Parachain,
  SwapInfo,
  TransferData,
} from '@galacticcouncil/xcm-core';

const isHydration = (c: AnyChain) =>
  c instanceof Parachain &&
  c.ecosystem === ChainEcosystem.Polkadot &&
  c.parachainId === 2034;

const isBasilisk = (c: AnyChain) =>
  c instanceof Parachain &&
  c.ecosystem === ChainEcosystem.Kusama &&
  c.parachainId === 2090;

const IS_DEX = (c: AnyChain) => isHydration(c) || isBasilisk(c);

const IS_HUB = (c: AnyChain) =>
  c instanceof Parachain && c.parachainId === 1000;

export class Dex {
  readonly chain: Parachain;
  readonly router: TradeRouter;

  constructor(configService: ConfigService, poolService: PoolService) {
    const chains = configService.chains.values();
    const dex = Array.from(chains).find(IS_DEX);
    if (dex) {
      this.chain = dex as Parachain;
    } else {
      throw new Error('DEX parachain config is missing');
    }

    this.router = new TradeRouter(poolService, {
      includeOnly: [PoolType.Omni, PoolType.Stable],
    });
  }

  isSwapSupported(fee: AssetAmount, transferData: TransferData): boolean {
    const { asset, source } = transferData;
    const destFee = this.parseDestFee(transferData);
    const isSupported = IS_HUB(source.chain) || IS_DEX(source.chain);
    const isSufficientAssetTransfer = asset.isEqual(destFee);
    const isFeePaymentAsset = fee.isSame(destFee);
    return isSupported && !isSufficientAssetTransfer && !isFeePaymentAsset;
  }

  async calculateFeeSwap(
    fee: AssetAmount,
    transferData: TransferData
  ): Promise<SwapInfo> {
    const { source } = transferData;
    const destFee = this.parseDestFee(transferData);
    const destFeeBalance = this.parseDestFeeBalance(transferData);
    const assetIn = this.chain.getMetadataAssetId(fee);
    const assetOut = this.chain.getMetadataAssetId(destFee);
    const amountOut = destFee.toDecimal(destFee.decimals);

    const trade = await this.router.getBestBuy(
      assetIn.toString(),
      assetOut.toString(),
      amountOut
    );

    const amountIn = BigInt(trade.amountIn.toNumber());
    const maxAmountIn = amountIn * 2n; // Support slippage up to 100%

    const hasNotEnoughDestFee = destFeeBalance.amount < destFee.amount;
    const hasEnoughReservesToSwap =
      source.feeBalance.amount - fee.amount > maxAmountIn;

    return {
      aIn: fee.copyWith({ amount: amountIn }),
      aOut: destFee,
      enabled: hasNotEnoughDestFee && hasEnoughReservesToSwap,
      route: buildRoute(trade.swaps),
    } as SwapInfo;
  }

  private parseDestFee(transferData: TransferData) {
    const { destination, via } = transferData;
    const routeFee = via && via.fee;
    return routeFee || destination.fee;
  }

  private parseDestFeeBalance(transferData: TransferData) {
    const { destination, via } = transferData;
    const routeFeeBalance = via && via.feeBalance;
    return routeFeeBalance || destination.feeBalance;
  }
}