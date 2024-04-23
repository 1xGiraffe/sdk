export interface AssetParams {
  key: string;
  originSymbol: string;
}

export class Asset {
  readonly key: string;

  readonly originSymbol: string;

  constructor({ key, originSymbol }: AssetParams) {
    this.key = key;
    this.originSymbol = originSymbol;
  }

  isEqual(asset: Asset): boolean {
    return this.key === asset.key && this.originSymbol === asset.originSymbol;
  }
}
