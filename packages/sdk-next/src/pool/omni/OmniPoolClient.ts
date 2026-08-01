import {
  AccountId,
  Binary,
  BlockInfo,
  CompatibilityLevel,
  Enum,
  SizedHex,
} from 'polkadot-api';
import { toHex } from '@polkadot-api/utils';

import {
  Observable,
  Subscription,
  bufferTime,
  concatMap,
  distinctUntilChanged,
  filter,
  finalize,
  from,
  map,
  merge,
  pairwise,
  switchMap,
  tap,
} from 'rxjs';

import { HYDRATION_SS58_PREFIX } from '@galacticcouncil/common';

import { PoolClient } from '../PoolClient';
import { PoolType, PoolLimits, PoolToken, PoolPair } from '../types';

import { SYSTEM_ASSET_ID } from '../../consts';
import { AssetBalance } from '../../types';
import { fmt, QueryBus } from '../../utils';

import { OmniPoolBase, OmniPoolFees, OmniPoolToken } from './OmniPool';
import { OmniPoolFee } from './OmniPoolFee';
import {
  TDynamicFees,
  TDynamicFeesConfig,
  TEmaOracle,
  TEmaPair,
  TOmnipoolAsset,
  TSlipFee,
} from './types';
import { getEmaPair } from './utils';

const { FeeUtils } = fmt;

const ORACLE_NAME = Binary.toHex(Binary.fromText('omnipool')) as SizedHex<8>;
const ORACLE_PERIOD = Enum('Short');

interface PoolStateEvent {
  block: BlockInfo;
  ids: number[];
}

export class OmniPoolClient extends PoolClient<OmniPoolBase> {
  private queryBus = new QueryBus();
  private block: number = 0;

  private poolStateSyncId = 0;
  private poolStateAppliedBlockByAsset = new Map<number, number>();

  private dynamicFeesConfig = this.queryBus.scope<
    [number],
    TDynamicFeesConfig | undefined
  >(
    'DynamicFees.AssetFeeConfiguration',
    (id) =>
      this.api.query.DynamicFees.AssetFeeConfiguration.getValue(id, {
        at: this.at,
      }),
    (id) => String(id)
  );

  private dynamicFees = this.queryBus.scope<[number], TDynamicFees | undefined>(
    'DynamicFees.AssetFee',
    (id) => this.api.query.DynamicFees.AssetFee.getValue(id, { at: this.at }),
    (id) => String(id),
    6 * 1000
  );

  private maxSlipFee = this.queryBus.scope<[], TSlipFee | undefined>(
    'Omnipool.SlipFee',
    () => this.apiNext.query.Omnipool.SlipFee.getValue({ at: this.at }),
    () => String('slipFee')
  );

  private emaOracles = this.queryBus.scope<[TEmaPair], TEmaOracle | undefined>(
    'EmaOracle.Oracles.Short',
    (pair) =>
      this.api.query.EmaOracle.Oracles.getValue(
        ORACLE_NAME,
        pair,
        ORACLE_PERIOD,
        { at: this.at }
      ),
    (pair) => pair.join(':'),
    6 * 1000
  );

  getPoolType(): PoolType {
    return PoolType.Omni;
  }

  private getPoolAddress() {
    const name = 'modlomnipool'.padEnd(32, '\0');
    const nameU8a = new TextEncoder().encode(name);
    const nameHex = toHex(nameU8a);
    return AccountId(HYDRATION_SS58_PREFIX).dec(nameHex);
  }

  private async getPoolLimits(): Promise<PoolLimits> {
    const [maxInRatio, maxOutRatio, minTradingLimit] = await Promise.all([
      this.api.constants.Omnipool.MaxInRatio(),
      this.api.constants.Omnipool.MaxOutRatio(),
      this.api.constants.Omnipool.MinimumTradingLimit(),
    ]);

    return {
      maxInRatio: maxInRatio,
      maxOutRatio: maxOutRatio,
      minTradingLimit: minTradingLimit,
    } as PoolLimits;
  }

  async isSupported(): Promise<boolean> {
    const staticApis = await this.api.getStaticApis();
    return staticApis.compat.query.Omnipool.Assets.isCompatible(
      CompatibilityLevel.BackwardsCompatible
    );
  }

  protected async loadPools(): Promise<OmniPoolBase[]> {
    const hubAssetId = await this.api.constants.Omnipool.HubAssetId();
    const poolAddress = this.getPoolAddress();

    const [
      entries,
      hubAssetTradeability,
      hubAssetMeta,
      hubAssetBalance,
      limits,
      block,
    ] = await Promise.all([
      this.api.query.Omnipool.Assets.getEntries({ at: this.at }),
      this.api.query.Omnipool.HubAssetTradability.getValue({ at: this.at }),
      this.api.query.AssetRegistry.Assets.getValue(hubAssetId, { at: this.at }),
      this.balance.getBalance(poolAddress, hubAssetId),
      this.getPoolLimits(),
      this.api.query.System.Number.getValue({ at: this.at }),
    ]);

    this.block = block;

    const poolTokens = entries.map(async ({ keyArgs, value }) => {
      const [id] = keyArgs;
      const { hub_reserve, shares, tradable, cap, protocol_shares } = value;

      const [meta, balance] = await Promise.all([
        this.api.query.AssetRegistry.Assets.getValue(id, { at: this.at }),
        this.balance.getBalance(poolAddress, id),
      ]);

      return {
        id: id,
        decimals: meta?.decimals,
        existentialDeposit: meta?.existential_deposit,
        balance: balance.transferable,
        cap: cap,
        hubReserves: hub_reserve,
        protocolShares: protocol_shares,
        shares: shares,
        tradeable: tradable,
        type: meta?.asset_type.type,
      } as OmniPoolToken;
    });

    const tokens = await Promise.all(poolTokens);

    // Adding LRNA info
    tokens.push({
      id: hubAssetId,
      decimals: hubAssetMeta?.decimals,
      existentialDeposit: hubAssetMeta?.existential_deposit,
      balance: hubAssetBalance.transferable,
      tradeable: hubAssetTradeability,
      type: hubAssetMeta?.asset_type.type,
    } as OmniPoolToken);

    return [
      {
        address: poolAddress,
        type: PoolType.Omni,
        hubAssetId: hubAssetId,
        tokens: tokens,
        ...limits,
      } as OmniPoolBase,
    ];
  }

  async getPoolFees(pair: PoolPair): Promise<OmniPoolFees> {
    const feeAsset = pair.assetOut;
    const protocolAsset = pair.assetIn;

    const slipFee = await this.maxSlipFee.get();
    const maxSlipFee = slipFee ?? 0;

    const feeConfiguration = await this.dynamicFeesConfig.get(feeAsset);
    if (feeConfiguration?.type === 'Fixed') {
      const { asset_fee, protocol_fee } = feeConfiguration.value;
      return {
        assetFee: FeeUtils.fromPermill(asset_fee),
        protocolFee: FeeUtils.fromPermill(protocol_fee),
        maxSlipFee: FeeUtils.fromPermill(maxSlipFee),
      };
    }

    const [
      dynamicFee,
      oracleAssetFee,
      oracleProtocolFee,
      assetFeeParams,
      protocolFeeParams,
    ] = await Promise.all([
      this.dynamicFees.get(feeAsset),
      this.emaOracles.get(getEmaPair(feeAsset)),
      this.emaOracles.get(getEmaPair(protocolAsset)),
      feeConfiguration
        ? feeConfiguration.value.asset_fee_params
        : this.api.constants.DynamicFees.AssetFeeParameters(),
      feeConfiguration
        ? feeConfiguration.value.protocol_fee_params
        : this.api.constants.DynamicFees.ProtocolFeeParameters(),
    ]);

    return OmniPoolFee.compute(
      pair,
      this.block,
      dynamicFee,
      oracleAssetFee,
      oracleProtocolFee,
      assetFeeParams,
      protocolFeeParams,
      maxSlipFee
    );
  }

  private subscribeEmaOracles(): Subscription {
    const [pool] = this.store.pools;

    // the omnipool oracle pairs we actually care about, keyed for O(1) lookup
    const wanted = new Set(
      pool.tokens.map((t) => getEmaPair(t.id).join(':'))
    );

    // one merkle-gated subscription prefix-scoped to ORACLE_NAME instead of
    // one watchValue per pair (~23 `value` storage reads/block -> ~1 merkle
    // probe/block, with descendant reads only when an omnipool oracle changes).
    return this.api.query.EmaOracle.Oracles.watchEntries(ORACLE_NAME, {
      at: 'best',
    })
      .pipe(
        distinctUntilChanged((_, current) => !current.deltas),
        map((value, index) => ({ value, index })),
        tap(({ value, index }) => {
          if (index > 0) {
            this.log.trace('emaOracle.Oracles', value.deltas?.upserted);
          }
        }),
        finalize(() => this.emaOracles.clear()),
        this.watchGuard('emaOracle.Oracles')
      )
      .subscribe(({ value: { deltas } }) => {
        deltas?.upserted.forEach((delta) => {
          const [, pair, period] = delta.args;
          if (period.type !== ORACLE_PERIOD.type) return;
          if (!wanted.has(pair.join(':'))) return;
          this.emaOracles.set(delta.value, pair);
        });
      });
  }

  private subscribeDynamicFees(): Subscription {
    return this.api.query.DynamicFees.AssetFee.watchEntries({
      at: 'best',
    })
      .pipe(
        distinctUntilChanged((_, current) => !current.deltas),
        map((value, index) => ({ value, index })),
        tap(({ value, index }) => {
          if (index > 0) {
            this.log.trace('dynamicFees.AssetFee', value.deltas?.upserted);
          }
        }),
        finalize(() => this.dynamicFees.clear()),
        this.watchGuard('dynamicFees.AssetFee')
      )
      .subscribe(({ value: { deltas } }) => {
        deltas?.upserted.forEach((delta) => {
          const [key] = delta.args;
          this.dynamicFees.set(delta.value, key);
        });
      });
  }

  private subscribeDynamicFeesConfig(): Subscription {
    return this.api.query.DynamicFees.AssetFeeConfiguration.watchEntries({
      at: 'best',
    })
      .pipe(
        distinctUntilChanged((_, current) => !current.deltas),
        map((value, index) => ({ value, index })),
        tap(({ value, index }) => {
          if (index > 0) {
            this.log.trace(
              'dynamicFees.AssetFeeConfiguration',
              value.deltas?.upserted
            );
          }
        }),
        finalize(() => this.dynamicFeesConfig.clear()),
        this.watchGuard('dynamicFees.AssetFeeConfiguration')
      )
      .subscribe(({ value: { deltas } }) => {
        deltas?.upserted.forEach((delta) => {
          const [key] = delta.args;
          this.dynamicFeesConfig.set(delta.value, key);
        });
      });
  }

  private subscribeBlock(): Subscription {
    return this.watcher.bestBlock$
      .pipe(this.watchGuard('watcher.bestBlock'))
      .subscribe((block) => {
        this.block = block;
      });
  }

  /**
   * Balance updates are folded into the block-consistent pool state sync
   * (`subscribePoolState`), so the base-class balance writer must stay off —
   * it applies balances without any block coordination.
   */
  protected subscribeBalances(): Subscription {
    return Subscription.EMPTY;
  }

  private watchOmnipoolAssets(): Observable<PoolStateEvent> {
    return this.api.query.Omnipool.Assets.watchEntries({
      at: 'best',
    }).pipe(
      distinctUntilChanged((_, current) => !current.deltas),
      map((value, index) => ({ value, index })),
      tap(({ value, index }) => {
        if (index > 0) {
          this.log.trace('omnipool.Assets', value.deltas?.upserted);
        }
      }),
      this.watchGuard('omnipool.Assets'),
      map(({ value: { block, deltas } }) => ({
        block,
        ids: (deltas?.upserted ?? []).map(({ args }) => args[0]),
      })),
      filter(({ ids }) => ids.length > 0)
    );
  }

  private watchTokenBalances(pool: OmniPoolBase): Observable<PoolStateEvent> {
    return this.api.query.Tokens.Accounts.watchEntries(pool.address, {
      at: 'best',
    }).pipe(
      distinctUntilChanged((_, current) => !current.deltas),
      map(({ block, deltas }) => ({
        block,
        ids: [
          ...(deltas?.deleted ?? []).map(({ args }) => args[1]),
          ...(deltas?.upserted ?? []).map(({ args }) => args[1]),
        ],
      })),
      filter(({ ids }) => ids.length > 0)
    );
  }

  private watchSystemBalance(pool: OmniPoolBase): Observable<PoolStateEvent> {
    return this.api.query.System.Account.watchValue(pool.address, {
      at: 'best',
    }).pipe(
      distinctUntilChanged((prev, curr) => {
        const p = prev.value.data;
        const c = curr.value.data;
        return (
          p.free === c.free &&
          p.reserved === c.reserved &&
          p.frozen === c.frozen
        );
      }),
      map((value, index) => ({ value, index })),
      filter(({ index }) => index > 0),
      map(({ value: { block } }) => ({ block, ids: [SYSTEM_ASSET_ID] }))
    );
  }

  private watchErc20Balances(pool: OmniPoolBase): Observable<PoolStateEvent> {
    const erc20Ids = pool.tokens
      .filter((t) => t.type === 'Erc20')
      .map((t) => t.id);

    return this.client.bestBlocks$.pipe(
      map(([best]) => best),
      switchMap((best) =>
        from(this.fetchPoolBalances(pool.address, erc20Ids, best.hash)).pipe(
          map((balances) => ({ block: best, balances }))
        )
      ),
      pairwise(),
      map(([prev, curr]) => {
        const deltas = this.balance.getDeltas(prev.balances, curr.balances);
        return { block: curr.block, ids: deltas.map(({ id }) => id) };
      }),
      filter(({ ids }) => ids.length > 0)
    );
  }

  private watchInitialPoolState(pool: OmniPoolBase): Observable<PoolStateEvent> {
    return from(this.client.getBestBlocks()).pipe(
      map(([best]) => ({ block: best, ids: pool.tokens.map(({ id }) => id) }))
    );
  }

  private subscribePoolState(): Subscription {
    const [pool] = this.store.pools;
    if (!pool) return Subscription.EMPTY;

    const syncId = ++this.poolStateSyncId;
    this.poolStateAppliedBlockByAsset.clear();

    const sources: Observable<PoolStateEvent>[] = [
      this.watchInitialPoolState(pool),
      this.watchOmnipoolAssets(),
      this.watchTokenBalances(pool),
    ];

    if (this.hasSystemAsset(pool)) {
      sources.push(this.watchSystemBalance(pool));
    }
    if (this.hasErc20Asset(pool)) {
      sources.push(this.watchErc20Balances(pool));
    }

    const sub = merge(...sources)
      .pipe(
        bufferTime(250),
        filter((events) => events.length > 0),
        concatMap((events) => from(this.syncPoolState(events, syncId))),
        this.watchGuard('omnipool.State')
      )
      .subscribe();

    sub.add(() => {
      if (this.poolStateSyncId === syncId) {
        this.poolStateSyncId++;
      }
    });

    return sub;
  }

  protected subscribeUpdates(): Subscription {
    const sub = new Subscription();

    sub.add(this.subscribePoolState());
    sub.add(this.subscribeDynamicFees());
    sub.add(this.subscribeDynamicFeesConfig());
    sub.add(this.subscribeEmaOracles());
    sub.add(this.subscribeBlock());

    return sub;
  }

  private isActivePoolStateSync(syncId: number): boolean {
    return this.poolStateSyncId === syncId;
  }

  private isStalePoolStateBlock(assetId: number, block: BlockInfo): boolean {
    const applied = this.poolStateAppliedBlockByAsset.get(assetId);
    return applied !== undefined && block.number < applied;
  }

  private markPoolStateBlockApplied(assetId: number, block: BlockInfo): void {
    this.poolStateAppliedBlockByAsset.set(assetId, block.number);
  }

  private async syncPoolState(
    events: PoolStateEvent[],
    syncId: number
  ): Promise<void> {
    const byBlock = new Map<string, { block: BlockInfo; ids: Set<number> }>();

    events.forEach(({ block, ids }) => {
      const group = byBlock.get(block.hash) ?? { block, ids: new Set() };
      ids.forEach((id) => group.ids.add(id));
      byBlock.set(block.hash, group);
    });

    const ordered = Array.from(byBlock.values()).sort(
      (a, b) => a.block.number - b.block.number
    );

    for (const { block, ids } of ordered) {
      if (!this.isActivePoolStateSync(syncId)) return;
      await this.syncPoolStateAt(block, Array.from(ids), syncId);
    }
  }

  private async syncPoolStateAt(
    block: BlockInfo,
    ids: number[],
    syncId: number
  ): Promise<void> {
    if (!this.isActivePoolStateSync(syncId)) return;

    const [pool] = this.store.pools;
    if (!pool) return;

    const known = new Set(pool.tokens.map((t) => t.id));
    const wanted = ids.filter(
      (id) => known.has(id) && !this.isStalePoolStateBlock(id, block)
    );
    if (wanted.length === 0) return;

    const at = block.hash;
    const nonHub = wanted.filter((id) => id !== pool.hubAssetId);

    const [balances, states] = await Promise.all([
      this.fetchPoolBalances(pool.address, wanted, at),
      Promise.all(
        nonHub.map(async (id) => {
          const state = await this.api.query.Omnipool.Assets.getValue(id, {
            at,
          });
          return [id, state] as const;
        })
      ),
    ]);

    if (!this.isActivePoolStateSync(syncId)) return;

    const balanceByAsset = new Map(
      balances.map(({ id, balance }) => [id, balance.transferable])
    );
    const stateByAsset = states.reduce((acc, [id, state]) => {
      if (state) acc.set(id, state);
      return acc;
    }, new Map<number, TOmnipoolAsset>());

    this.store.update(([current]) => {
      if (!this.isActivePoolStateSync(syncId)) return [];
      if (!current) return [];

      const fresh = wanted.filter((id) => !this.isStalePoolStateBlock(id, block));
      if (fresh.length === 0) return [];

      const freshSet = new Set(fresh);
      const applied = new Set<number>();

      this.block = Math.max(this.block, block.number);

      const tokens = current.tokens.map((token) => {
        if (!freshSet.has(token.id)) return token;

        const withBalance = {
          ...token,
          balance: balanceByAsset.get(token.id) ?? token.balance,
        };
        const state = stateByAsset.get(token.id);

        applied.add(token.id);
        return state ? this.updateTokenState(withBalance, state) : withBalance;
      });

      applied.forEach((id) => this.markPoolStateBlockApplied(id, block));
      return [{ ...current, tokens }];
    });
  }

  private async fetchPoolBalances(
    address: string,
    ids: number[],
    at: string
  ): Promise<AssetBalance[]> {
    return await Promise.all(
      ids.map(async (id) => ({
        id,
        balance: await this.balance.getBalance(address, id, at),
      }))
    );
  }

  private updateTokenState(token: PoolToken, state: TOmnipoolAsset) {
    const { hub_reserve, shares, tradable, cap, protocol_shares } = state;
    return {
      ...token,
      cap: cap,
      hubReserves: hub_reserve,
      protocolShares: protocol_shares,
      shares: shares,
      tradeable: tradable,
    } as OmniPoolToken;
  }
}
