import { Parachain } from '@galacticcouncil/xcm-core';
import { TypeRegistry, Enum, Struct } from '@polkadot/types';

const registry = new TypeRegistry();

class VersionedUserAction extends Enum {
  constructor(value: any) {
    super(registry, { V1: XcmRoutingUserAction }, value);
  }
}

class XcmRoutingUserAction extends Struct {
  constructor(value: any) {
    super(registry, { destination: 'VersionedMultiLocation' }, value);
  }
}

export function createMRLPayload(
  parachain: Parachain,
  account: string,
  isEthereumStyle = false
): VersionedUserAction {
  const multilocation = {
    V1: {
      parents: 1,
      interior: {
        X2: [
          { Parachain: parachain.parachainId },
          isEthereumStyle
            ? { AccountKey20: { key: account } }
            : { AccountId32: { id: account } },
        ],
      },
    },
  };

  const versionedMultilocation = registry.createType(
    'VersionedMultiLocation',
    multilocation
  );

  const userAction = new XcmRoutingUserAction({
    destination: versionedMultilocation,
  });
  return new VersionedUserAction({ V1: userAction });
}
