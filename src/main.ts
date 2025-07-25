// Apify SDK - toolkit for building Apify Actors (Read more at https://docs.apify.com/sdk/js/).
import {
  ApiItemResponse,
  createLinkedinScraper,
  Profile,
  ProfileShort,
  ScrapeLinkedinProfilesParams,
} from '@harvestapi/scraper';
import { Actor } from 'apify';
import { config } from 'dotenv';
import { styleText } from 'node:util';

config();

// The init() call configures the Actor for its environment. It's recommended to start every Actor with an init().
await Actor.init();

enum ProfileScraperMode {
  SHORT,
  FULL,
  EMAIL,
}

const profileScraperModeInputMap1: Record<string, ProfileScraperMode> = {
  'Short ($2 per 1k)': ProfileScraperMode.SHORT,
  'Full ($6 per 1k)': ProfileScraperMode.FULL,
  'Full + email search ($10 per 1k)': ProfileScraperMode.EMAIL,
};
const profileScraperModeInputMap2: Record<string, ProfileScraperMode> = {
  '1': ProfileScraperMode.SHORT,
  '2': ProfileScraperMode.FULL,
  '3': ProfileScraperMode.EMAIL,
};

interface Input {
  profileScraperMode: string;
  currentCompanies?: string[];
  pastCompanies?: string[];
  firstName?: string;
  lastName?: string;
  schools?: string[];
  locations?: string[];
  industryIds?: string[];
  maxItems?: number;
}

// Structure of input is defined in input_schema.json
const input = await Actor.getInput<Input>();
if (!input) throw new Error('Input is missing!');

input.firstName = (input.firstName || '').trim();
input.lastName = (input.lastName || '').trim();

if (!input.firstName || !input.lastName) {
  console.warn(
    styleText('bgYellow', ' [WARNING] ') + ' Please provide firstName and lastName inputs.',
  );
  await Actor.exit();
  process.exit(0);
}

const profileScraperMode =
  profileScraperModeInputMap1[input.profileScraperMode] ??
  profileScraperModeInputMap2[input.profileScraperMode] ??
  ProfileScraperMode.FULL;

const query: {
  currentCompany: string[];
  pastCompany: string[];
  school: string[];
  location: string[];
  firstName: string;
  lastName: string;
  industryId?: string[];
} = {
  currentCompany: input.currentCompanies || [],
  pastCompany: input.pastCompanies || [],
  school: input.schools || [],
  location: input.locations || [],
  firstName: input.firstName,
  lastName: input.lastName,
  industryId: input.industryIds || [],
};

for (const key of Object.keys(query) as (keyof typeof query)[]) {
  if (Array.isArray(query[key]) && query[key].length) {
    (query[key] as string[]) = query[key]
      .map((v) => (v || '').replace(/,/g, ' ').replace(/\s+/g, ' ').trim())
      .filter((v) => v && v.length);
  }
}

const { actorId, actorRunId, actorBuildId, userId, actorMaxPaidDatasetItems, memoryMbytes } =
  Actor.getEnv();
const client = Actor.newClient();

const user = userId ? await client.user(userId).get() : null;
const cm = Actor.getChargingManager();
const pricingInfo = cm.getPricingInfo();
const isPaying = (user as Record<string, any> | null)?.isPaying === false ? false : true;

const state: {
  lastPromise: Promise<any> | null;
  leftItems: number;
  scrapedItems: number;
} = {
  scrapedItems: 0,
  lastPromise: null,
  leftItems: actorMaxPaidDatasetItems || 1000000,
};
if (input.maxItems && input.maxItems < state.leftItems) {
  state.leftItems = input.maxItems;
}

let isFreeUserExceeding = false;
const logFreeUserExceeding = () =>
  console.warn(
    styleText('bgYellow', ' [WARNING] ') +
      ' Free users are limited up to 10 items per run. Please upgrade to a paid plan to scrape more items.',
  );

if (!isPaying) {
  if (state.leftItems > 10) {
    isFreeUserExceeding = true;
    state.leftItems = 10;
    logFreeUserExceeding();
  }
}

const pushItem = async (item: Profile | ProfileShort, payments: string[]) => {
  console.info(`Scraped profile ${item.linkedinUrl || item?.publicIdentifier || item?.id}`);
  state.scrapedItems += 1;

  if (pricingInfo.isPayPerEvent) {
    if (profileScraperMode === ProfileScraperMode.SHORT) {
      state.lastPromise = Actor.pushData(item, 'short-profile');
    }
    if (profileScraperMode === ProfileScraperMode.FULL) {
      state.lastPromise = Actor.pushData(item, 'full-profile');
    }
    if (profileScraperMode === ProfileScraperMode.EMAIL) {
      if ((payments || []).includes('linkedinProfileWithEmail')) {
        state.lastPromise = Actor.pushData(item, 'full-profile-with-email');
      } else {
        state.lastPromise = Actor.pushData(item, 'full-profile');
      }
    }
  } else {
    state.lastPromise = Actor.pushData(item);
  }
};

const scraper = createLinkedinScraper({
  apiKey: process.env.HARVESTAPI_TOKEN!,
  baseUrl: process.env.HARVESTAPI_URL || 'https://api.harvest-api.com',
  addHeaders: {
    'x-apify-userid': userId!,
    'x-apify-actor-id': actorId!,
    'x-apify-actor-run-id': actorRunId!,
    'x-apify-actor-build-id': actorBuildId!,
    'x-apify-memory-mbytes': String(memoryMbytes),
    'x-apify-actor-max-paid-dataset-items': String(actorMaxPaidDatasetItems) || '0',
    'x-apify-username': user?.username || '',
    'x-apify-user-is-paying': (user as Record<string, any> | null)?.isPaying,
    'x-apify-user-is-paying2': String(isPaying),
    'x-apify-max-total-charge-usd': String(pricingInfo.maxTotalChargeUsd),
    'x-apify-is-pay-per-event': String(pricingInfo.isPayPerEvent),
    'x-apify-user-left-items': String(state.leftItems),
    'x-apify-user-max-items': String(input.maxItems),
  },
});

const scrapeParams: Omit<ScrapeLinkedinProfilesParams, 'query'> = {
  findEmail: profileScraperMode === ProfileScraperMode.EMAIL,
  outputType: 'callback',
  onItemScraped: async ({ item, payments }) => {
    return pushItem(item, payments || []);
  },
  optionsOverride: {
    fetchItem: async ({ item }) => {
      if (item?.id || item?.linkedinUrl) {
        state.leftItems -= 1;
        if (state.leftItems < 0) {
          return { skipped: true, done: true };
        }

        if (profileScraperMode === ProfileScraperMode.SHORT && item?.linkedinUrl) {
          return {
            status: 200,
            entityId: item.id || item.publicIdentifier,
            element: item,
          } as ApiItemResponse<Profile>;
        }

        return scraper.getProfile({
          url: `https://www.linkedin.com/in/${item.publicIdentifier || item.id}`,
          findEmail: profileScraperMode === ProfileScraperMode.EMAIL,
        });
      }

      return { skipped: true };
    },
  },
  disableLog: true,
  overrideConcurrency: profileScraperMode === ProfileScraperMode.EMAIL ? 10 : 8,
  overridePageConcurrency: 2,
};

if (state.leftItems <= 0) {
  console.warn(
    styleText('bgYellow', ' [WARNING] ') +
      ' No items left to scrape. Please increase the maxItems input or reduce the filters.',
  );
  await Actor.exit();
  process.exit(0);
}

const itemQuery = {
  search: `${query.firstName} ${query.lastName}`.trim(),
  ...query,
};
for (const key of Object.keys(itemQuery) as (keyof typeof itemQuery)[]) {
  if (!itemQuery[key]) {
    delete itemQuery[key];
  }
  if (Array.isArray(itemQuery[key])) {
    if (!itemQuery[key].length) {
      delete itemQuery[key];
    }
  }
}

let requestSuccess = false;

await scraper.scrapeProfiles({
  query: itemQuery,
  ...scrapeParams,
  maxItems: state.leftItems,
  onFirstPageFetched: ({ data }) => {
    if (data?.status === 429) {
      console.error('Too many requests');
    } else if (data?.pagination) {
      requestSuccess = true;
      console.info(`Found ${data.pagination.totalElements} profiles total.`);
    }
  },

  addListingHeaders: {
    'x-sub-user': (isPaying ? user?.username : '') || '',
    'x-concurrency': (isPaying ? '' : '1') || '',
    'x-queue-size': isPaying ? '20' : '5',
  },
});

if (state.scrapedItems <= 5 && requestSuccess) {
  Actor.charge({ eventName: 'actor-start' });
}

await state.lastPromise;

if (isFreeUserExceeding) {
  logFreeUserExceeding();
}

// Gracefully exit the Actor process. It's recommended to quit all Actors with an exit().
await Actor.exit();
// process.exit(0);
