export const ELEMENT_ID = 'initialization-data';
export const PREFIX_PLACEHOLDER = '__PATH_PREFIX__';
export const TELEMETRY_PLACEHOLDER = '__TELEMETRY_ENABLED__';
export const LIVE_DATA_POLL_RATE_PLACEHOLDER = '__LIVE_DATA_POLL_RATE__';
export const INSTANCE_ID_PLACEHOLDER = '__INSTANCE_ID__';
export const ENVIRONMENT_BANNER_PLACEHOLDER = '__ENVIRONMENT_BANNER__';

type InitializationData = {
  pathPrefix: string;
  telemetryEnabled: boolean;
  liveDataPollRate?: number;
  instanceId: string;
  environmentBanner?: {text: string; color: string};
};

let value: InitializationData | undefined;

// Read settings that the webserver inserts into index.html.
export const extractInitializationData = (): InitializationData => {
  if (!value) {
    value = {pathPrefix: '', telemetryEnabled: false, instanceId: ''};
    const element = document.getElementById(ELEMENT_ID);
    if (element) {
      const parsed = JSON.parse(element.innerHTML);
      if (parsed.pathPrefix !== PREFIX_PLACEHOLDER) {
        value.pathPrefix = parsed.pathPrefix;
      }
      if (parsed.telemetryEnabled !== TELEMETRY_PLACEHOLDER) {
        value.telemetryEnabled = parsed.telemetryEnabled;
      }
      if (parsed.liveDataPollRate !== LIVE_DATA_POLL_RATE_PLACEHOLDER) {
        value.liveDataPollRate = parsed.liveDataPollRate;
      }
      if (parsed.instanceId !== INSTANCE_ID_PLACEHOLDER) {
        value.instanceId = parsed.instanceId;
      }
      const {environmentBanner} = parsed;
      if (
        typeof environmentBanner?.text === 'string' &&
        typeof environmentBanner.color === 'string'
      ) {
        value.environmentBanner = environmentBanner;
      }
    }
  }
  if (value.telemetryEnabled) {
    const script = document.createElement('script');
    script.defer = true;
    script.async = true;
    script.src = 'https://dagster.io/oss-telemetry.js';
    document.head.appendChild(script);
  }
  return value;
};
