import { ConfigManagerV2 } from '../../services/config-manager-v2';
import { AvailableNetworks } from '../../services/config-manager-types';

export namespace DfxConfig {
  export interface NetworkConfig {
    allowedSlippage: string;
    gasLimit: number;
    ttl: number;
    routerAddress: (network: string) => string;
    tradingTypes: Array<string>;
    availableNetworks: Array<AvailableNetworks>;
  }

  export const config: NetworkConfig = {
    allowedSlippage: ConfigManagerV2.getInstance().get('dfx.allowedSlippage'),
    gasLimit: ConfigManagerV2.getInstance().get('dfx.gasLimit'),
    ttl: ConfigManagerV2.getInstance().get('dfx.ttl'),
    routerAddress: (network: string) =>
      ConfigManagerV2.getInstance().get(
        `dfx.contractAddresses.${network}.routerAddress`
      ),
    tradingTypes: ['EVM_AMM'],
    availableNetworks: [{ chain: 'ethereum', networks: ['mainnet'] }],
  };
}
