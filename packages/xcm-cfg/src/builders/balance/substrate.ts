import {
  BalanceConfigBuilder,
  Parachain,
  SubstrateQueryConfig,
} from '@galacticcouncil/xcm-core';
import { Option } from '@polkadot/types';
import {
  FrameSystemAccountInfo,
  OrmlTokensAccountData,
  PalletBalancesAccountData,
  PalletAssetsAssetAccount,
} from '@polkadot/types/lookup';

export function substrate() {
  return {
    assets,
    system,
    tokens,
    ormlTokens,
    foreignAssets,
  };
}

function assets() {
  return {
    account: (): BalanceConfigBuilder => ({
      build: ({ address, asset, chain }) => {
        const assetId = chain.getBalanceAssetId(asset);
        return new SubstrateQueryConfig({
          module: 'assets',
          func: 'account',
          args: [assetId, address],
          transform: async (
            response: Option<PalletAssetsAssetAccount>
          ): Promise<bigint> => response.unwrapOrDefault().balance.toBigInt(),
        });
      },
    }),
  };
}

function foreignAssets() {
  return {
    account: (): BalanceConfigBuilder => ({
      build: ({ address, asset, chain }) => {
        const ctx = chain as Parachain;

        const assetLocation = ctx.getAssetXcmLocation(asset);
        if (!assetLocation) {
          throw new Error('Missing asset xcm location for ' + asset.key);
        }

        return new SubstrateQueryConfig({
          module: 'foreignAssets',
          func: 'account',
          args: [assetLocation, address],
          transform: async (
            response: Option<PalletAssetsAssetAccount>
          ): Promise<bigint> => response.unwrapOrDefault().balance.toBigInt(),
        });
      },
    }),
  };
}

function system() {
  return {
    account: (): BalanceConfigBuilder => ({
      build: ({ address }) =>
        new SubstrateQueryConfig({
          module: 'system',
          func: 'account',
          args: [address],
          transform: async (
            response: FrameSystemAccountInfo
          ): Promise<bigint> => {
            const balance = response.data as PalletBalancesAccountData;
            // @ts-ignore
            const frozen = balance.miscFrozen ?? balance.frozen;
            return BigInt(balance.free.sub(frozen).toString());
          },
        }),
    }),
  };
}

function tokens() {
  return {
    accounts: (): BalanceConfigBuilder => ({
      build: ({ address, asset, chain }) => {
        const assetId = chain.getBalanceAssetId(asset);
        return new SubstrateQueryConfig({
          module: 'tokens',
          func: 'accounts',
          args: [address, assetId],
          transform: async ({
            free,
            frozen,
          }: OrmlTokensAccountData): Promise<bigint> =>
            BigInt(free.sub(frozen).toString()),
        });
      },
    }),
  };
}

function ormlTokens() {
  return {
    accounts: (): BalanceConfigBuilder => ({
      build: ({ address, asset, chain }) => {
        const assetId = chain.getBalanceAssetId(asset);
        return new SubstrateQueryConfig({
          module: 'ormlTokens',
          func: 'accounts',
          args: [address, assetId],
          transform: async ({
            free,
            frozen,
          }: OrmlTokensAccountData): Promise<bigint> =>
            BigInt(free.sub(frozen).toString()),
        });
      },
    }),
  };
}
