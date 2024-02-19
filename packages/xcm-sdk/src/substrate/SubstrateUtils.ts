import { Asset, AssetAmountParams } from '@moonbeam-network/xcm-types';
import { convertDecimals } from '@moonbeam-network/xcm-utils';

import { SubstrateService } from './SubstrateService';

/**
 * Normalize asset amount if chain uses solely chain decimals
 * for balance representation of assets.
 *
 * @param amount - original chain amount
 * @param asset - asset
 * @param substrate - source chain substrate service
 * @returns normalized asset amount if chain decimals used, otherwise default
 */
export function normalizeAssetAmount(
  amount: bigint,
  asset: Asset,
  substrate: SubstrateService
): AssetAmountParams {
  const chainDecimals = substrate.decimals;
  const assetDecimals = substrate.getDecimals(asset);
  const normDecimals = substrate.chain.usesChainDecimals
    ? chainDecimals
    : assetDecimals;
  const normBalance = convertDecimals(amount, normDecimals, assetDecimals);
  return {
    amount: normBalance,
    decimals: assetDecimals,
  };
}