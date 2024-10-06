import {
  Asset,
  AssetConfig,
  ExtrinsicConfigBuilderParams,
} from '@galacticcouncil/xcm-core';

import { glmr, usdt } from '../../../assets';
import {
  ContractBuilder,
  ExtrinsicBuilder,
  ExtrinsicDecorator,
  FeeAmountBuilder,
} from '../../../builders';
import { assetHub, ethereum, moonbeam, zeitgeist } from '../../../chains';

import { balance, fee } from './configs';

export const MRL_EXECUTION_FEE = 0.9;
export const MRL_XCM_FEE = 1;

const isSwapSupported = (params: ExtrinsicConfigBuilderParams) => {
  const { source } = params;
  const { enabled } = source.feeSwap || {};
  return !!enabled;
};

const swapExtrinsic = ExtrinsicBuilder().router().buy({ withSlippage: 30 });

export function toHubExtTemplate(asset: Asset): AssetConfig {
  return new AssetConfig({
    asset: asset,
    balance: balance(),
    destination: assetHub,
    destinationFee: {
      amount: 0.18,
      asset: usdt,
      balance: balance(),
    },
    extrinsic: ExtrinsicDecorator(isSwapSupported, swapExtrinsic).prior(
      ExtrinsicBuilder().xTokens().transferMultiassets().X3()
    ),
    fee: fee(),
  });
}

export function toMoonbeamErc20Template(asset: Asset): AssetConfig {
  return new AssetConfig({
    asset: asset,
    balance: balance(),
    destination: moonbeam,
    destinationFee: {
      amount: 0.08,
      asset: glmr,
      balance: balance(),
    },
    extrinsic: ExtrinsicDecorator(isSwapSupported, swapExtrinsic).prior(
      ExtrinsicBuilder().xTokens().transferMultiCurrencies()
    ),
    fee: fee(),
  });
}

export function toZeitgeistErc20Template(asset: Asset): AssetConfig {
  return new AssetConfig({
    asset: asset,
    balance: balance(),
    destination: zeitgeist,
    destinationFee: {
      amount: 0.1,
      asset: glmr,
      balance: balance(),
    },
    extrinsic: ExtrinsicDecorator(isSwapSupported, swapExtrinsic).prior(
      ExtrinsicBuilder().xTokens().transferMultiCurrencies()
    ),
    fee: fee(),
  });
}

export function toEthereumWithRelayerTemplate(asset: Asset): AssetConfig {
  return new AssetConfig({
    asset: asset,
    balance: balance(),
    destination: ethereum,
    destinationFee: {
      amount: FeeAmountBuilder().TokenRelayer().calculateRelayerFee(),
      asset: asset,
      balance: balance(),
    },
    extrinsic: ExtrinsicDecorator(isSwapSupported, swapExtrinsic).priorMulti([
      ExtrinsicBuilder().xTokens().transferMultiCurrencies(),
      ExtrinsicBuilder()
        .polkadotXcm()
        .send()
        .transact({ fee: MRL_EXECUTION_FEE }),
    ]),
    fee: fee(),
    via: {
      chain: moonbeam,
      fee: {
        amount: MRL_XCM_FEE,
        asset: glmr,
        balance: balance(),
      },
      transact: ExtrinsicBuilder()
        .ethereumXcm()
        .transact(
          ContractBuilder()
            .Batch()
            .batchAll([
              ContractBuilder().Erc20().approve(),
              ContractBuilder().TokenRelayer().transferTokensWithRelay(),
            ])
        ),
    },
  });
}
