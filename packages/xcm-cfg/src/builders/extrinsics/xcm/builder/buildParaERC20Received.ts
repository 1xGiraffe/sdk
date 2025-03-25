import { Asset, Parachain } from '@galacticcouncil/xcm-core';

import { ACCOUNT_ID_32, AMOUNT_MAX, DOT_LOCATION, TOPIC } from './const';

import { getExtrinsicAssetLocation, locationOrError } from '../utils';
import { XcmVersion } from '../types';

export async function buildParaERC20Received(asset: Asset, chain: Parachain) {
  const api = await chain.api;

  const version = XcmVersion.v4;
  const transferAssetLocation = getExtrinsicAssetLocation(
    locationOrError(chain, asset),
    version
  );

  return api.createType('XcmVersionedXcm', {
    [version]: [
      {
        ReserveAssetDeposited: [
          {
            id: DOT_LOCATION,
            fun: {
              Fungible: AMOUNT_MAX,
            },
          },
          {
            id: transferAssetLocation,
            fun: {
              Fungible: AMOUNT_MAX,
            },
          },
        ],
      },
      { ClearOrigin: null },
      {
        BuyExecution: {
          fees: {
            id: DOT_LOCATION,
            fun: {
              Fungible: AMOUNT_MAX,
            },
          },
          weightLimit: 'Unlimited',
        },
      },
      {
        DepositAsset: {
          assets: {
            Wild: {
              AllCounted: 2,
            },
          },
          beneficiary: {
            parents: 0,
            interior: {
              X1: [
                {
                  AccountId32: {
                    id: ACCOUNT_ID_32,
                    network: null,
                  },
                },
              ],
            },
          },
        },
      },
      {
        SetTopic: TOPIC,
      },
    ],
  });
}
