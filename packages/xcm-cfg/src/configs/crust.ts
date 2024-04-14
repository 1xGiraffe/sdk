import { AssetConfig, ChainConfig } from '@galacticcouncil/xcm-core';
import {
  BalanceBuilder,
  ExtrinsicBuilder,
} from '@moonbeam-network/xcm-builder';

import { cru } from '../assets';
import { hydraDX, crust } from '../chains';

const toHydraDX: AssetConfig[] = [
  new AssetConfig({
    asset: cru,
    balance: BalanceBuilder().substrate().system().account(),
    destination: hydraDX,
    destinationFee: {
      amount: 0.01,
      asset: cru,
      balance: BalanceBuilder().substrate().system().account(),
    },
    extrinsic: ExtrinsicBuilder().xTokens().transfer(),
  }),
];

export const crustConfig = new ChainConfig({
  assets: [...toHydraDX],
  chain: crust,
});
