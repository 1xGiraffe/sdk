import { AnyChain, Ecosystem, EvmParachain, Parachain } from '@moonbeam-network/xcm-types';
import {
  astar as defaultAstar,
  bifrostPolkadot as defaultBifrostPolkadot,
  centrifuge as defaultCentrifuge,
  hydraDX as defaultHydraDx,
  interlay as defaultInterlay,
  moonbeam as defaultMoonbeam,
  polkadot,
  polkadotAssetHub as defaultPolkadotAssetHub,
} from '@moonbeam-network/xcm-config';

import { astr, bnc, cfg, daiAcala, daiMoonbeam, dot, glmr, hdx, ibtc, usdt, wbtc, weth, ztg } from './assets';

export const acala = new EvmParachain({
  assetsData: [
    {
      asset: daiAcala,
      balanceId: '0x54a37a01cd75b616d63e0ab665bffdb0143c52ae',
      id: { Erc20: '0x54a37a01cd75b616d63e0ab665bffdb0143c52ae' },
    },
  ],
  ecosystem: Ecosystem.Polkadot,
  genesisHash: '0xfc41b9bd8ef8fe53d58c7ea67c794c7ec9a73daf05e6d54b14ff6342c99ba64c',
  id: 2000,
  key: 'acala',
  name: 'Acala',
  parachainId: 2000,
  ss58Format: 10,
  ws: 'wss://acala-rpc-0.aca-api.network',
  rpc: 'wss://eth-rpc-acala.aca-api.network/ws',
});

export const assetHub = new Parachain({
  ...defaultPolkadotAssetHub,
  assetsData: [
    {
      asset: usdt,
      id: 1984,
      palletInstance: 50,
    },
  ],
  key: 'assethub',
  name: 'AssetHub',
});

export const astar = new Parachain({
  ...defaultAstar,
  assetsData: [
    {
      asset: astr,
      metadataId: 0,
    },
  ],
});

export const bifrost = new Parachain({
  ...defaultBifrostPolkadot,
  assetsData: [
    {
      asset: bnc,
      id: { Native: bnc.originSymbol },
    },
  ],
  key: 'bifrost',
});

export const centrifuge = new Parachain({
  ...defaultCentrifuge,
  assetsData: [
    {
      asset: cfg,
      id: 'Native',
    },
  ],
});

export const hydraDX = new Parachain({
  ...defaultHydraDx,
  key: 'hydradx',
  //ws: 'wss://hydradx-rpc.dwellir.com',
  assetsData: [
    {
      asset: astr,
      id: 9,
    },
    {
      asset: bnc,
      id: 14,
    },
    {
      asset: cfg,
      id: 13,
    },
    {
      asset: dot,
      id: 5,
    },
    {
      asset: daiAcala,
      id: 2,
    },
    {
      asset: daiMoonbeam,
      id: 18,
    },
    {
      asset: glmr,
      id: 16,
    },
    {
      asset: ibtc,
      id: 11,
    },
    {
      asset: hdx,
      id: 0,
    },
    {
      asset: usdt,
      id: 1984,
      balanceId: 10,
      metadataId: 10,
      palletInstance: 50,
    },
    {
      asset: wbtc,
      id: 19,
    },
    {
      asset: weth,
      id: 20,
    },
    {
      asset: ztg,
      id: 12,
    },
  ],
});

export const interlay = new Parachain({
  ...defaultInterlay,
  assetsData: [
    {
      asset: ibtc,
      decimals: 8,
      id: { Token: ibtc.originSymbol },
      metadataId: 0,
    },
  ],
});

export const moonbeam = new EvmParachain({
  ...defaultMoonbeam,
  assetsData: [
    {
      asset: daiMoonbeam,
      id: '0x06e605775296e851FF43b4dAa541Bb0984E9D6fD',
      metadataId: 0,
    },
    {
      asset: glmr,
      id: '0x0000000000000000000000000000000000000802',
      min: 0.1,
    },
    {
      asset: hdx,
      id: '69606720909260275826784788104880799692',
    },
    {
      asset: wbtc,
      id: '0xE57eBd2d67B462E9926e04a8e33f01cD0D64346D',
      metadataId: 0,
    },
    {
      asset: weth,
      id: '0xab3f0245B83feB11d15AAffeFD7AD465a59817eD',
      metadataId: 0,
    },
  ],
});

export const zeitgeist = new Parachain({
  assetsData: [
    {
      asset: ztg,
      id: 'Ztg',
    },
  ],
  ecosystem: Ecosystem.Polkadot,
  genesisHash: '',
  key: 'zeitgeist',
  name: 'Zeitgeist',
  parachainId: 2092,
  ss58Format: 73,
  ws: 'wss://zeitgeist.api.onfinality.io/public-ws',
});

export const chains: AnyChain[] = [
  acala,
  assetHub,
  astar,
  bifrost,
  centrifuge,
  hydraDX,
  interlay,
  moonbeam,
  polkadot,
  zeitgeist,
];

export const chainsMap = new Map<string, AnyChain>(
  chains.map((chain) => [chain.key, chain]),
);