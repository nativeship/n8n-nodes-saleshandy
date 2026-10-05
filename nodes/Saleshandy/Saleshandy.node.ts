import { NodeConnectionTypes, NodeApiError, NodeOperationError, type IDataObject, type IExecuteFunctions, type IHttpRequestOptions, type INodeExecutionData, type INodeType, type INodeTypeDescription, type JsonObject } from "n8n-workflow";
import { requestWithRetry, resolveServerBaseUrl } from "../../shared/http";

// Generated with ts-morph
type CredentialApplication = { credentialType: string; type: 'apiKey' | 'basic' | 'bearer' | 'oauth2' | 'custom'; location?: 'header' | 'query'; parameter?: string; injections?: Array<{ target: 'header' | 'query' | 'body'; name: string; value: string }> };
type RetryContract = { mode: string; retryConnectionFailures?: boolean; retryTimeouts?: boolean; retryRateLimits?: boolean; retryServerErrors?: boolean; maxAttempts: number; maxElapsedMs: number; baseBackoffMs: number; maxBackoffMs: number; jitterRatio: number; idempotency?: { target: 'header' | 'query' | 'body'; parameter: string } };
type PaginationContract = { style: string; page?: string; limit?: string; cursor?: string; responseCursor?: string; hasMore?: string; itemPath?: string; advancement?: string; maxPages: number; maxItems: number; maxElapsedMs: number; maxMemoryBytes: number; repeatedCursorLimit: number; repeatedPageLimit: number; pageSize: number };

function normalizeParameterValue(value: unknown): IDataObject[string] {
  if (value && typeof value === 'object' && 'value' in value) return (value as { value: IDataObject[string] }).value;
  return value as IDataObject[string];
}


type BodyFieldContract = {
  name: string;
  displayName?: string;
  description?: string;
  placeholder?: string;
  type?: string;
  format?: string;
  required?: boolean;
  minValue?: number;
  maxValue?: number;
  enum?: unknown[];
  default?: unknown;
  example?: unknown;
  pattern?: string;
  fields?: BodyFieldContract[];
  items?: BodyFieldContract;
  additionalValue?: BodyFieldContract;
  alternatives?: BodyFieldContract[];
  composition?: 'oneOf' | 'anyOf';
  representation?: string;
  nullable?: boolean;
};

function normalizeJsonValue(value: unknown, label: string, context: IExecuteFunctions, itemIndex: number): IDataObject | IDataObject[] | string | number | boolean | null {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return {};
    try {
      return JSON.parse(trimmed) as IDataObject | IDataObject[] | string | number | boolean | null;
    } catch (error) {
      throw new NodeOperationError(context.getNode(), `${label} must be valid JSON: ${(error as Error).message}`, { itemIndex });
    }
  }
  if (value === null || Array.isArray(value) || (value && typeof value === 'object') || typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return value as IDataObject | IDataObject[] | string | number | boolean | null;
  throw new NodeOperationError(context.getNode(), `${label} must be valid JSON`, { itemIndex });
}


function validateBodyValue(value: unknown, contract: BodyFieldContract, path: string, context: IExecuteFunctions, itemIndex: number): void {
  if (value === undefined || value === '') {
    if (contract.required) throw new NodeOperationError(context.getNode(), `${path} is required`, { itemIndex });
    return;
  }
  if (value === null) {
    if (contract.nullable) return;
    throw new NodeOperationError(context.getNode(), `${path} must not be null`, { itemIndex });
  }
  if (contract.alternatives?.length) {
    selectAlternativeValue(value, contract, path, context, itemIndex);
    return;
  }
  if (contract.type === 'string' && typeof value !== 'string') throw new NodeOperationError(context.getNode(), `${path} must be a string`, { itemIndex });
  if (contract.type === 'boolean' && typeof value !== 'boolean') throw new NodeOperationError(context.getNode(), `${path} must be a boolean`, { itemIndex });
  if (contract.type === 'number' && typeof value !== 'number') throw new NodeOperationError(context.getNode(), `${path} must be a number`, { itemIndex });
  if (contract.type === 'integer' && (typeof value !== 'number' || !Number.isInteger(value))) throw new NodeOperationError(context.getNode(), `${path} must be an integer`, { itemIndex });
  if (contract.enum?.length) {
    const enumValueMatches = (candidate: unknown): boolean => candidate === value ||
      (candidate === null && value === 'null') ||
      (candidate === 'null' && value === null) ||
      Boolean(candidate && value && typeof candidate === 'object' && typeof value === 'object' && JSON.stringify(candidate) === JSON.stringify(value));
    const scalarEnum = contract.enum.every((candidate) => candidate === null || ['string', 'number', 'boolean'].includes(typeof candidate));
    const matches = contract.type === 'array' && Array.isArray(value) && scalarEnum
      ? value.every((item) => contract.enum!.some((candidate) => candidate === item || (candidate === null && item === 'null') || (candidate === 'null' && item === null)))
      : contract.enum.some(enumValueMatches);
    if (!matches) throw new NodeOperationError(context.getNode(), `${path} must be one of: ${contract.enum.join(', ')}`, { itemIndex });
  }
  if (contract.type === 'number' || contract.type === 'integer') {
    const numeric = value as number;
    if (contract.minValue !== undefined && numeric < contract.minValue) throw new NodeOperationError(context.getNode(), `${path} must be at least ${contract.minValue}`, { itemIndex });
    if (contract.maxValue !== undefined && numeric > contract.maxValue) throw new NodeOperationError(context.getNode(), `${path} must be at most ${contract.maxValue}`, { itemIndex });
  }
  if (contract.pattern && typeof value === 'string' && !new RegExp(contract.pattern).test(value)) throw new NodeOperationError(context.getNode(), `${path} must match ${contract.pattern}`, { itemIndex });
  if (contract.format === 'email' && typeof value === 'string' && !/^[^@\s]+@[^@\s]+\.[^@\s]+$/u.test(value)) throw new NodeOperationError(context.getNode(), `${path} must be an email address`, { itemIndex });
  if ((contract.format === 'uri' || contract.format === 'url') && typeof value === 'string') {
    try {
      new URL(value);
    } catch {
      throw new NodeOperationError(context.getNode(), `${path} must be a URL`, { itemIndex });
    }
  }
  if (contract.format === 'uuid' && typeof value === 'string' && !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(value)) throw new NodeOperationError(context.getNode(), `${path} must be a UUID`, { itemIndex });
  if (contract.type === 'object') {
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new NodeOperationError(context.getNode(), `${path} must be a JSON object`, { itemIndex });
    const objectValue = value as IDataObject;
    for (const child of contract.fields ?? []) validateBodyValue(objectValue[child.name], child, `${path}.${child.name}`, context, itemIndex);
    if (contract.additionalValue) {
      const known = new Set((contract.fields ?? []).map((field) => field.name));
      for (const [key, childValue] of Object.entries(objectValue)) {
        if (!known.has(key)) {
          if (contract.additionalValue.alternatives?.length && contract.additionalValue.representation === 'raw') continue;
          validateBodyValue(childValue, contract.additionalValue, `${path}.${key}`, context, itemIndex);
        }
      }
    }
  }
  if (contract.type === 'array') {
    if (!Array.isArray(value)) throw new NodeOperationError(context.getNode(), `${path} must be a JSON array`, { itemIndex });
    if (contract.items) value.forEach((item, index) => validateBodyValue(item, contract.items!, `${path}[${index}]`, context, itemIndex));
  }
}

function setBodyField(body: IDataObject, contract: BodyFieldContract, value: unknown, context: IExecuteFunctions, itemIndex: number): void {
  const normalized = contract.type === 'object' || contract.type === 'array' || contract.type === 'alternative' || contract.representation === 'raw'
    ? normalizeJsonValue(value, contract.displayName ?? contract.name, context, itemIndex)
    : normalizeParameterValue(value);
  const selected = contract.alternatives?.length ? selectAlternativeValue(normalized, contract, contract.name, context, itemIndex) : normalized;
  validateBodyValue(selected, { ...contract, alternatives: undefined, composition: undefined }, contract.name, context, itemIndex);
  body[contract.name] = selected as IDataObject[string];
}


function selectAlternativeValue(value: unknown, contract: BodyFieldContract, path: string, context: IExecuteFunctions, itemIndex: number): unknown {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new NodeOperationError(context.getNode(), `${path} must include an explicit schema alternative and value`, { itemIndex });
  const selectedName = String((value as IDataObject).schemaAlternative ?? '');
  const selected = (contract.alternatives ?? []).find((alternative) => alternative.name === selectedName);
  if (!selected) throw new NodeOperationError(context.getNode(), `${path} schema alternative must be one of: ${(contract.alternatives ?? []).map((alternative) => alternative.name).join(', ')}`, { itemIndex });
  const selectedValue = (value as IDataObject).value;
  validateBodyValue(selectedValue, selected, path, context, itemIndex);
  return selectedValue;
}

function encodeFormValue(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value) || (value && typeof value === 'object')) return JSON.stringify(value);
  return String(value);
}



function toFormData(body: IDataObject): FormData {
  const form = new FormData();
  for (const [key, value] of Object.entries(body)) form.append(key, encodeFormValue(value));
  return form;
}

function selectResponseFields(value: IDataObject, fields: string[]): IDataObject {
  if (fields.length === 0) return value;
  const selected: IDataObject = {};
  if (value.id !== undefined) selected.id = value.id;
  for (const field of fields) if (value[field] !== undefined) selected[field] = value[field];
  return selected;
}

function valueAtPath(value: unknown, path: string): unknown {
  if (!path) return value;
  return path.split('.').filter(Boolean).reduce((current: unknown, segment) => {
    if (current === undefined || current === null) return undefined;
    if (Array.isArray(current)) return current[Number(segment)];
    return (current as IDataObject)[segment];
  }, value);
}

export class Saleshandy implements INodeType {
  description: INodeTypeDescription = {
        displayName: "Saleshandy",
        name: "saleshandy",
        icon: {
            light: "file:saleshandy.svg",
            dark: "file:saleshandy.dark.svg"
        },
        group: [],
        version: [
            1
        ],
        subtitle: "={{((JSON.parse(\"\\u007b\\\"analytics\\\":\\u007b\\\"AnalyticsController_exportEmailSentDetails\\\":\\\"exportSentEmailDetails: analytic\\\",\\\"AnalyticsController_getAllTeamSummarizedStats\\\":\\\"getTeamAnalytics: analytic\\\",\\\"AnalyticsController_getEmailAccountStats\\\":\\\"getEmailAccountAnalytics: analytic\\\",\\\"AnalyticsController_getSequenceConsolidatedReport\\\":\\\"getSequenceEngagementAnalytics: analytic\\\",\\\"AnalyticsController_getSequenceStats\\\":\\\"getSequenceAnalytics: analytic\\\"\\u007d,\\\"attachments\\\":\\u007b\\\"AttachmentController_uploadAttachment\\\":\\\"uploadAnAttachment: attachment\\\"\\u007d,\\\"blacklistDomains\\\":\\u007b\\\"DomainBlacklistController_addBlacklistDomain\\\":\\\"blacklistDomains: blacklistDomain\\\"\\u007d,\\\"clients\\\":\\u007b\\\"ClientController_assignResource\\\":\\\"assignResourcesToAClient: client\\\",\\\"ClientController_createClient\\\":\\\"createAClient: client\\\",\\\"ClientController_getClientLists\\\":\\\"listClients: client\\\"\\u007d,\\\"doNotContact\\\":\\u007b\\\"DncController_addItemsToDncList\\\":\\\"addItemsToADoNotContactList: doNotContact\\\",\\\"DncController_getDncById\\\":\\\"getItemsInADoNotContactList: doNotContact\\\",\\\"DncController_getDncListWithItem\\\":\\\"searchDoNotContactItems: doNotContact\\\",\\\"DncController_getDncLists\\\":\\\"listDoNotContactLists: doNotContact\\\"\\u007d,\\\"emailAccounts\\\":\\u007b\\\"EmailAccountController_addEmailAccounts\\\":\\\"connectNewSendingEmailAccountsSmtpAndImap: emailAccount\\\",\\\"EmailAccountController_connectEmailAccount\\\":\\\"connectAnSmtpImapEmailAccount: emailAccount\\\",\\\"EmailAccountController_getEmailAccounts\\\":\\\"listSendingEmailAccounts: emailAccount\\\",\\\"EmailAccountController_getEmailAccountsConnectStatus\\\":\\\"checkEmailAccountConnectionStatus: emailAccount\\\",\\\"EmailAccountController_reconnectEmailAccounts\\\":\\\"reconnectExistingEmailAccounts: emailAccount\\\",\\\"EmailAccountController_updateEmailAccounts\\\":\\\"updateSendingAccountsInBulk: emailAccount\\\"\\u007d,\\\"enrichment\\\":\\u007b\\\"LeadFinderController_aiChat\\\":\\\"searchLeadsWithAi: enrichment\\\",\\\"LeadFinderController_bulkAddLeadsToSequence\\\":\\\"addLeadsToASequence: enrichment\\\",\\\"LeadFinderController_enrichCompanies\\\":\\\"enrichCompanies: enrichment\\\",\\\"LeadFinderController_enrichLeads\\\":\\\"enrichPeople: enrichment\\\",\\\"LeadFinderController_getCreditDetails\\\":\\\"getCreditBalanceAndUsage: enrichment\\\",\\\"LeadFinderController_getEnrichmentResult\\\":\\\"getEnrichmentResults: enrichment\\\",\\\"LeadFinderController_getEnrichmentStatus\\\":\\\"getEnrichmentJobStatus: enrichment\\\",\\\"LeadFinderController_getLeadFinderFilters\\\":\\\"listLeadSearchFilters: enrichment\\\",\\\"LeadFinderController_getRateLimitStatus\\\":\\\"getApiRateLimits: enrichment\\\",\\\"LeadFinderController_searchCompanies\\\":\\\"searchCompanies: enrichment\\\",\\\"LeadFinderController_searchLeads\\\":\\\"searchPeople: enrichment\\\"\\u007d,\\\"fields\\\":\\u007b\\\"FieldController_createField\\\":\\\"createACustomField: field\\\",\\\"FieldController_getFields\\\":\\\"listAllFields: field\\\",\\\"FieldController_updateField\\\":\\\"updateACustomField: field\\\"\\u007d,\\\"leadsToEmail\\\":\\u007b\\\"LeadsToEmailController_createDomainsWorkflow\\\":\\\"createADomainBasedLeadWorkflow: leadsToEmail\\\",\\\"LeadsToEmailController_createWorkflow\\\":\\\"createALeadsToEmailWorkflow: leadsToEmail\\\"\\u007d,\\\"notes\\\":\\u007b\\\"NoteController_createNote\\\":\\\"createANote: note\\\",\\\"NoteController_updateNote\\\":\\\"updateANote: note\\\",\\\"NoteController_uploadNoteAttachment\\\":\\\"uploadAnAttachmentForANote: note\\\"\\u007d,\\\"prospects\\\":\\u007b\\\"ProspectController_getProspectAttributeById\\\":\\\"getAProspectAttributeValue: prospect\\\",\\\"ProspectController_getProspectImportStatus\\\":\\\"checkProspectImportStatus: prospect\\\",\\\"ProspectController_getProspectNotes\\\":\\\"listNotesForAProspect: prospect\\\",\\\"ProspectController_getProspectsVerificationStatus\\\":\\\"checkEmailVerificationStatus: prospect\\\",\\\"ProspectController_importProspects\\\":\\\"importProspects: prospect\\\",\\\"ProspectController_importProspectsV2\\\":\\\"importProspectsUsingFieldNames: prospect\\\",\\\"ProspectController_upsertAttribute\\\":\\\"updateProspectFieldValues: prospect\\\",\\\"SequenceContactController_assignTagsToProspects\\\":\\\"assignTagsToProspects: prospect\\\",\\\"SequenceContactController_findProspects\\\":\\\"listProspects: prospect\\\",\\\"SequenceContactController_findTags\\\":\\\"listProspectTags: prospect\\\",\\\"SequenceContactController_getContactMinimalSequences\\\":\\\"getAContactSequenceHistory: prospect\\\",\\\"SequenceContactController_unAssignTagsToProspects\\\":\\\"removeTagsFromProspects: prospect\\\",\\\"SequenceContactController_unsubscribeProspects\\\":\\\"handleAProspectUnsubscribeRequest: prospect\\\",\\\"SequenceContactController_unsubscribeProspects-postV1ProspectsUnsubscribe\\\":\\\"unsubscribeProspects: prospect\\\",\\\"SequenceContactController_updateProspects\\\":\\\"updateProspectStatuses: prospect\\\"\\u007d,\\\"schedules\\\":\\u007b\\\"ScheduleController_createSchedule\\\":\\\"createASchedule: schedule\\\",\\\"ScheduleController_getSchedules\\\":\\\"listAllSchedules: schedule\\\"\\u007d,\\\"sequences\\\":\\u007b\\\"SequenceController_addContactsToSequence\\\":\\\"addProspectsToASequenceStep: sequence\\\",\\\"SequenceController_addEmailAccountToSequence\\\":\\\"addAccountsToASequence: sequence\\\",\\\"SequenceController_createSequence\\\":\\\"createANewSequence: sequence\\\",\\\"SequenceController_createStep\\\":\\\"createANewStepForASequence: sequence\\\",\\\"SequenceController_createVariant\\\":\\\"addANewVariantToAnExistingStep: sequence\\\",\\\"SequenceController_getSequenceSettings\\\":\\\"getSequenceSettingsOptionallyFilterByCode: sequence\\\",\\\"SequenceController_getSequenceStepVariants\\\":\\\"getSequenceStepVariants: sequence\\\",\\\"SequenceController_getSequenceSteps\\\":\\\"listAllStepsAndVariantsForASequence: sequence\\\",\\\"SequenceController_getSequencesWithSteps\\\":\\\"listSequencesAndSteps: sequence\\\",\\\"SequenceController_importProspectsV2\\\":\\\"importProspectsIntoASequenceUsingFieldNames: sequence\\\",\\\"SequenceController_removeEmailAccountFromSequence\\\":\\\"removeAccountsFromASequence: sequence\\\",\\\"SequenceController_sendTestEmail\\\":\\\"sendASequenceTestEmail: sequence\\\",\\\"SequenceController_sequenceEmailAccountList\\\":\\\"listSequenceSendingAccounts: sequence\\\",\\\"SequenceController_updatePriorityDistribution\\\":\\\"updatePriorityDistributionForASequence: sequence\\\",\\\"SequenceController_updateProspectOutcome\\\":\\\"updateProspectOutcomes: sequence\\\",\\\"SequenceController_updateSequenceSchedule\\\":\\\"updateTheScheduleAssignedToASequence: sequence\\\",\\\"SequenceController_updateSequenceSettings\\\":\\\"updateSequenceSettingsAndOrSchedule: sequence\\\",\\\"SequenceController_updateSequenceStatus\\\":\\\"pauseOrResumeSequences: sequence\\\",\\\"SequenceController_updateVariant\\\":\\\"updateAStepVariantForASequence: sequence\\\",\\\"SequenceController_verifyProspects\\\":\\\"startProspectEmailVerification: sequence\\\"\\u007d,\\\"subsequences\\\":\\u007b\\\"SubsequenceController_createSubsequence\\\":\\\"createANewSubsequenceUnderAParentSequence: subsequence\\\",\\\"SubsequenceController_getSubsequenceSettings\\\":\\\"getScheduleEntryDelayTriggerConditions: subsequence\\\",\\\"SubsequenceController_listSubsequences\\\":\\\"listAllSubsequencesUnderAParentSequence: subsequence\\\",\\\"SubsequenceController_updateSubsequence\\\":\\\"updateTheScheduleEntryDelayOrConditions: subsequence\\\"\\u007d,\\\"tasks\\\":\\u007b\\\"TaskController_bulkSkipTask\\\":\\\"skipMultipleTasksAtTheSameTime: task\\\",\\\"TaskController_bulkSnoozeTask\\\":\\\"snoozeMultipleTasksWithSnoozeDurationOrTime: task\\\",\\\"TaskController_completeTask\\\":\\\"completeATask: task\\\",\\\"TaskController_createTask\\\":\\\"createATask: task\\\",\\\"TaskController_getAssigneeList\\\":\\\"listTaskAssignees: task\\\",\\\"TaskController_getBulkTaskStatus\\\":\\\"getBulkTaskStatus: task\\\",\\\"TaskController_getTaskById\\\":\\\"getTaskDetails: task\\\",\\\"TaskController_getTaskCounts\\\":\\\"getTaskCounts: task\\\",\\\"TaskController_getTasks\\\":\\\"listTasks: task\\\",\\\"TaskController_skipTask\\\":\\\"skipATask: task\\\",\\\"TaskController_snoozeTask\\\":\\\"snoozeATask: task\\\",\\\"TaskController_updateTaskNote\\\":\\\"updateATaskNote: task\\\"\\u007d,\\\"unibox\\\":\\u007b\\\"UniboxController_getCategories\\\":\\\"listEmailReplyCategories: unibox\\\"\\u007d,\\\"unifiedInbox\\\":\\u007b\\\"UnifiedInboxController_getAll\\\":\\\"getTheUnreadThreadCount: unifiedInbox\\\",\\\"UnifiedInboxController_getAllEmails\\\":\\\"getAnEmailThread: unifiedInbox\\\",\\\"UnifiedInboxController_getEmailContentForSingleEmail\\\":\\\"getEmailContent: unifiedInbox\\\",\\\"UnifiedInboxController_getEmailList\\\":\\\"listInboxEmails: unifiedInbox\\\",\\\"UnifiedInboxController_getFields\\\":\\\"listInboxOutcomes: unifiedInbox\\\",\\\"UnifiedInboxController_replyOnEmail\\\":\\\"replyToAnEmailThread: unifiedInbox\\\"\\u007d\\u007d\"))[$parameter[\"resource\"]] || {})[$parameter[\"operation\"]] || ($parameter[\"operation\"] + \": \" + $parameter[\"resource\"])}}",
        description: "Saleshandy helps sales teams find prospects and manage cold email outreach.",
        documentationUrl: "https://api.example.com",
        hints: [
            {
                message: "Operation \"ClientController_getClientLists\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"DncController_getDncLists\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"DncController_getDncListWithItem\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"DncController_getDncById\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"DomainController_searchDomains\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"SequenceContactController_findProspects\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"SequenceController_getSequencesWithSteps\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"TaskController_getTasks\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"UnifiedInboxController_getFields\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"WebhookController_listWebhooks\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "The specification does not declare a server URL; generated routing uses a configurable self-hosted destination. Set the generated node's HTTPS Destination URL to the API host before executing it.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            }
        ],
        defaults: {
            name: "Saleshandy"
        },
        usableAsTool: true,
        inputs: [
            NodeConnectionTypes.Main
        ],
        outputs: [
            NodeConnectionTypes.Main
        ],
        credentials: [
            {
                name: "saleshandyApi",
                required: true
            }
        ],
        properties: [
            {
                displayName: "Resource",
                name: "resource",
                type: "options",
                noDataExpression: true,
                default: "analytics",
                options: [
                    {
                        name: "Analytic",
                        value: "analytics"
                    },
                    {
                        name: "Attachment",
                        value: "attachments"
                    },
                    {
                        name: "Blacklist Domain",
                        value: "blacklistDomains"
                    },
                    {
                        name: "Client",
                        value: "clients"
                    },
                    {
                        name: "Do Not Contact",
                        value: "doNotContact"
                    },
                    {
                        name: "Email Account",
                        value: "emailAccounts"
                    },
                    {
                        name: "Enrichment",
                        value: "enrichment"
                    },
                    {
                        name: "Field",
                        value: "fields"
                    },
                    {
                        name: "Leads To Email",
                        value: "leadsToEmail"
                    },
                    {
                        name: "Note",
                        value: "notes"
                    },
                    {
                        name: "Prospect",
                        value: "prospects"
                    },
                    {
                        name: "Schedule",
                        value: "schedules"
                    },
                    {
                        name: "Sequence",
                        value: "sequences"
                    },
                    {
                        name: "Subsequence",
                        value: "subsequences"
                    },
                    {
                        name: "Task",
                        value: "tasks"
                    },
                    {
                        name: "Unibox",
                        value: "unibox"
                    },
                    {
                        name: "Unified Inbox",
                        value: "unifiedInbox"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ]
                    }
                },
                default: "AnalyticsController_exportEmailSentDetails",
                options: [
                    {
                        name: "Export Sent Email Details",
                        value: "AnalyticsController_exportEmailSentDetails",
                        action: "Export sent email details analytics",
                        description: "Export sent email details for the selected sequences and date range as a CSV sent to your account email. analytics."
                    },
                    {
                        name: "Get Email Account",
                        value: "AnalyticsController_getEmailAccountStats",
                        action: "Get email account analytics",
                        description: "Return engagement, sentiment, and performance statistics for the specified email account. analytics."
                    },
                    {
                        name: "Get Sequence",
                        value: "AnalyticsController_getSequenceStats",
                        action: "Get sequence analytics",
                        description: "Return prospect and email performance statistics for the specified sequence. analytics."
                    },
                    {
                        name: "Get Sequence Engagement",
                        value: "AnalyticsController_getSequenceConsolidatedReport",
                        action: "Get sequence engagement analytics",
                        description: "Return engagement statistics across the selected sequences and date range. analytics."
                    },
                    {
                        name: "Get Team",
                        value: "AnalyticsController_getAllTeamSummarizedStats",
                        action: "Get team analytics",
                        description: "Return team performance statistics for the selected users and reporting period. analytics."
                    }
                ]
            },
            {
                displayName: "End Date",
                name: "endDate",
                type: "string",
                default: "",
                required: true,
                description: "End date for the report (ISO date string)",
                placeholder: "e.g. 2024-01-31",
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_exportEmailSentDetails"
                        ]
                    }
                }
            },
            {
                displayName: "Start Date",
                name: "startDate",
                type: "string",
                default: "",
                required: true,
                description: "Start date for the report (ISO date string)",
                placeholder: "e.g. 2024-01-01",
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_exportEmailSentDetails"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_exportEmailSentDetails"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Sequence IDs",
                        name: "sequenceIds",
                        type: "json",
                        default: [],
                        description: "Filter by specific sequence IDs"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_exportEmailSentDetails"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Count By",
                name: "countBy",
                type: "options",
                default: "relative",
                required: true,
                description: "How to group the statistics (relative or absolute)",
                placeholder: "e.g. absolute",
                options: [
                    {
                        name: "Absolute",
                        value: "absolute"
                    },
                    {
                        name: "Relative",
                        value: "relative"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getAllTeamSummarizedStats"
                        ]
                    }
                }
            },
            {
                displayName: "End Date",
                name: "endDate",
                type: "string",
                default: "",
                required: true,
                description: "End date for the report period in ISO 8601 format with timezone",
                placeholder: "e.g. 2024-01-31T23:59:59.999+00:00",
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getAllTeamSummarizedStats"
                        ]
                    }
                }
            },
            {
                displayName: "Limit",
                name: "limit",
                type: "number",
                default: 50,
                required: true,
                description: "Max number of results to return",
                placeholder: "e.g. 10",
                typeOptions: {
                    minValue: 1
                },
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getAllTeamSummarizedStats"
                        ]
                    }
                }
            },
            {
                displayName: "Order By",
                name: "orderBy",
                type: "options",
                default: "prospectAdded",
                required: true,
                description: "Field name to order the results by. valid values depend on report type.",
                placeholder: "e.g. prospectAdded",
                options: [
                    {
                        name: "Bounced",
                        value: "bounced"
                    },
                    {
                        name: "Clicked",
                        value: "clicked"
                    },
                    {
                        name: "Closed",
                        value: "Closed"
                    },
                    {
                        name: "Do Not Contact",
                        value: "Do Not Contact"
                    },
                    {
                        name: "EmailSent",
                        value: "emailSent"
                    },
                    {
                        name: "Interested",
                        value: "Interested"
                    },
                    {
                        name: "Meeting Booked",
                        value: "Meeting Booked"
                    },
                    {
                        name: "Not Interested",
                        value: "Not Interested"
                    },
                    {
                        name: "Not Now",
                        value: "Not Now"
                    },
                    {
                        name: "Opened",
                        value: "opened"
                    },
                    {
                        name: "Out Of Office",
                        value: "Out of Office"
                    },
                    {
                        name: "ProspectAdded",
                        value: "prospectAdded"
                    },
                    {
                        name: "ProspectContacted",
                        value: "prospectContacted"
                    },
                    {
                        name: "Replied",
                        value: "replied"
                    },
                    {
                        name: "Uncategorized",
                        value: "Uncategorized"
                    },
                    {
                        name: "UnSubscribed",
                        value: "unSubscribed"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getAllTeamSummarizedStats"
                        ]
                    }
                }
            },
            {
                displayName: "Page",
                name: "page",
                type: "number",
                default: 0,
                required: true,
                description: "Page number for pagination",
                placeholder: "e.g. 1",
                typeOptions: {
                    minValue: 1
                },
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getAllTeamSummarizedStats"
                        ]
                    }
                }
            },
            {
                displayName: "Sort Order",
                name: "sortOrder",
                type: "options",
                default: "desc",
                required: true,
                description: "Sort direction for ordering results",
                placeholder: "e.g. desc",
                options: [
                    {
                        name: "Asc",
                        value: "asc"
                    },
                    {
                        name: "Desc",
                        value: "desc"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getAllTeamSummarizedStats"
                        ]
                    }
                }
            },
            {
                displayName: "Start Date",
                name: "startDate",
                type: "string",
                default: "",
                required: true,
                description: "Start date for the report period in ISO 8601 format with timezone",
                placeholder: "e.g. 2024-01-01T00:00:00.000+00:00",
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getAllTeamSummarizedStats"
                        ]
                    }
                }
            },
            {
                displayName: "Type",
                name: "type",
                type: "options",
                default: "prospect",
                required: true,
                description: "Type of report to generate (prospect or email)",
                placeholder: "e.g. prospect",
                options: [
                    {
                        name: "Email",
                        value: "email"
                    },
                    {
                        name: "Prospect",
                        value: "prospect"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getAllTeamSummarizedStats"
                        ]
                    }
                }
            },
            {
                displayName: "User IDs",
                name: "userIds",
                type: "json",
                default: [],
                required: true,
                description: "Array of encrypted user IDs to filter the report",
                placeholder: "e.g. abc123xyz,def456uvw",
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getAllTeamSummarizedStats"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getAllTeamSummarizedStats"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Email ID",
                name: "emailId",
                type: "string",
                default: "",
                required: true,
                description: "ID of the email account",
                placeholder: "e.g. 2dP27N0gZ4",
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getEmailAccountStats"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getEmailAccountStats"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "End Date",
                name: "endDate",
                type: "string",
                default: "",
                required: true,
                description: "End date must in a valid date & it must not exceed 1 year after start date. example:- 2001-12-18 (yyyy-mm-dd).",
                placeholder: "e.g. 2024-11-19",
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getSequenceConsolidatedReport"
                        ]
                    }
                }
            },
            {
                displayName: "Page Limit",
                name: "pageLimit",
                type: "number",
                default: 0,
                required: true,
                description: "Number of documents to be returned. page limit should be between 10 & 500 per page.",
                placeholder: "e.g. 25",
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getSequenceConsolidatedReport"
                        ]
                    }
                }
            },
            {
                displayName: "Page Num",
                name: "pageNum",
                type: "number",
                default: 0,
                required: true,
                description: "Page number. it should be greater then 1.",
                placeholder: "e.g. 1",
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getSequenceConsolidatedReport"
                        ]
                    }
                }
            },
            {
                displayName: "Sequence IDs",
                name: "sequenceIds",
                type: "json",
                default: [],
                required: true,
                description: "IDs of the sequences",
                placeholder: "e.g. 2dP27N0gZ4,2dP27NugZ3,2dP27NrgZ3",
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getSequenceConsolidatedReport"
                        ]
                    }
                }
            },
            {
                displayName: "Start Date",
                name: "startDate",
                type: "string",
                default: "",
                required: true,
                description: "Start date must in a valid date & it must not be older than 2 years ago. example:- 2001-12-18 (yyyy-mm-dd).",
                placeholder: "e.g. 2024-11-15",
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getSequenceConsolidatedReport"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getSequenceConsolidatedReport"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "ID of the sequence",
                placeholder: "e.g. 2dP27N0gZ4",
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getSequenceStats"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "analytics"
                        ],
                        operation: [
                            "AnalyticsController_getSequenceStats"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "attachments"
                        ]
                    }
                },
                default: "AttachmentController_uploadAttachment",
                options: [
                    {
                        name: "Upload An",
                        value: "AttachmentController_uploadAttachment",
                        action: "Upload attachment",
                        description: "Upload a file as multipart/form-data with a file field. use the returned attachmentid in an email-step variant; use post /v1/notes/attachments for note files."
                    }
                ]
            },
            {
                displayName: "File",
                name: "file",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "attachments"
                        ],
                        operation: [
                            "AttachmentController_uploadAttachment"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "attachments"
                        ],
                        operation: [
                            "AttachmentController_uploadAttachment"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "blacklistDomains"
                        ]
                    }
                },
                default: "DomainBlacklistController_addBlacklistDomain",
                options: [
                    {
                        name: "Blacklist Domains",
                        value: "DomainBlacklistController_addBlacklistDomain",
                        action: "Blacklist domains",
                        description: "Add the supplied domains to the account blacklist. blacklist domains."
                    }
                ]
            },
            {
                displayName: "Domains",
                name: "domains",
                type: "string",
                default: "",
                required: true,
                description: "List of domain names to be blacklisted seperated by ',' (not prefixed by https:// or https://). maximum domain limit is 50 & minumum domain limit is 1.",
                placeholder: "e.g. example.com,dev.to,rataalada.com",
                displayOptions: {
                    show: {
                        resource: [
                            "blacklistDomains"
                        ],
                        operation: [
                            "DomainBlacklistController_addBlacklistDomain"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "blacklistDomains"
                        ],
                        operation: [
                            "DomainBlacklistController_addBlacklistDomain"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ]
                    }
                },
                default: "ClientController_assignResource",
                options: [
                    {
                        name: "Assign Resources To A",
                        value: "ClientController_assignResource",
                        action: "Assign resources to a client",
                        description: "Assign the specified sequences or email accounts to a client"
                    },
                    {
                        name: "Create A",
                        value: "ClientController_createClient",
                        action: "Create client",
                        description: "Create a client using the supplied name, email, company, and permission details"
                    },
                    {
                        name: "List",
                        value: "ClientController_getClientLists",
                        action: "List clients",
                        description: "List clients with optional search, pagination, and sorting"
                    }
                ]
            },
            {
                displayName: "Resource Type",
                name: "resourceType",
                type: "options",
                default: "sequence",
                required: true,
                description: "Type of resource to assign",
                options: [
                    {
                        name: "EmailAccount",
                        value: "emailAccount"
                    },
                    {
                        name: "Sequence",
                        value: "sequence"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_assignResource"
                        ]
                    }
                }
            },
            {
                displayName: "Resource IDs",
                name: "resourceIds",
                type: "json",
                default: [],
                required: true,
                description: "IDs of resources to assign",
                placeholder: "e.g. 1Gz3xlNwr9,ajzR8xpPAq",
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_assignResource"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_assignResource"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Client ID",
                        name: "clientId",
                        type: "string",
                        default: "",
                        description: "Optional client ID for scoping",
                        placeholder: "e.g. 9pa8BRwy2"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_assignResource"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Company Name",
                name: "companyName",
                type: "string",
                default: "",
                required: true,
                description: "Client's company name",
                placeholder: "e.g. Acme Growth",
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_createClient"
                        ]
                    }
                }
            },
            {
                displayName: "Email",
                name: "email",
                type: "string",
                default: "",
                required: true,
                description: "Client's email address. must be a valid email on a non-blacklisted domain.",
                placeholder: "e.g. maya.client@example.org",
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_createClient"
                        ]
                    }
                }
            },
            {
                displayName: "First Name",
                name: "firstName",
                type: "string",
                default: "",
                required: true,
                description: "Client's first name",
                placeholder: "e.g. Maya",
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_createClient"
                        ]
                    }
                }
            },
            {
                displayName: "Last Name",
                name: "lastName",
                type: "string",
                default: "",
                required: true,
                description: "Client's last name",
                placeholder: "e.g. Client",
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_createClient"
                        ]
                    }
                }
            },
            {
                displayName: "Permission",
                name: "permission",
                type: "options",
                default: "1",
                required: true,
                description: "Access level for the client. accepted values: \"1\" = full access, \"2\" = limited access.",
                placeholder: "e.g. 1",
                options: [
                    {
                        name: "1",
                        value: "1"
                    },
                    {
                        name: "2",
                        value: "2"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_createClient"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_createClient"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sort",
                name: "sort",
                type: "options",
                default: "ASC",
                required: true,
                description: "Sort order for the results",
                placeholder: "e.g. DESC",
                options: [
                    {
                        name: "ASC",
                        value: "ASC"
                    },
                    {
                        name: "DESC",
                        value: "DESC"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_getClientLists"
                        ]
                    }
                }
            },
            {
                displayName: "Sort By",
                name: "sortBy",
                type: "options",
                default: "createdDate",
                required: true,
                description: "Field to sort the results by. if not selected, default sorting is done by createdat.",
                placeholder: "e.g. createdDate",
                options: [
                    {
                        name: "ActiveEmailAccounts",
                        value: "activeEmailAccounts"
                    },
                    {
                        name: "ActiveSequence",
                        value: "activeSequence"
                    },
                    {
                        name: "CompanyName",
                        value: "companyName"
                    },
                    {
                        name: "CreatedDate",
                        value: "createdDate"
                    },
                    {
                        name: "EmailSent",
                        value: "emailSent"
                    },
                    {
                        name: "FirstName",
                        value: "firstName"
                    },
                    {
                        name: "TotalProspects",
                        value: "totalProspects"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_getClientLists"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_getClientLists"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 1,
                        description: "Page number for pagination",
                        placeholder: "e.g. 1"
                    },
                    {
                        displayName: "Page Size",
                        name: "pageSize",
                        type: "number",
                        default: 25,
                        description: "Number of items per page. maximum pagesize is 100.",
                        placeholder: "e.g. 25"
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Search by first name, last name, email and company name",
                        placeholder: "e.g. John"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "clients"
                        ],
                        operation: [
                            "ClientController_getClientLists"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ]
                    }
                },
                default: "DncController_addItemsToDncList",
                options: [
                    {
                        name: "Add Items To A Do Not Contact List",
                        value: "DncController_addItemsToDncList",
                        action: "Add items to a do not contact list do not contact",
                        description: "Add the supplied domains or email addresses to a do-not-contact list. do not contact."
                    },
                    {
                        name: "Get Items In A Do Not Contact List",
                        value: "DncController_getDncById",
                        action: "Get items in a do not contact list do not contact",
                        description: "Return items from a do-not-contact list, with optional search and item type filters. do not contact."
                    },
                    {
                        name: "List Do Not Contact Lists",
                        value: "DncController_getDncLists",
                        action: "List do not contact lists do not contact",
                        description: "List do-not-contact lists with optional search, pagination, and sorting. do not contact."
                    },
                    {
                        name: "Search Do Not Contact Items",
                        value: "DncController_getDncListWithItem",
                        action: "Search do not contact items do not contact",
                        description: "Search for a do-not-contact item across all lists. do not contact."
                    }
                ]
            },
            {
                displayName: "Dnc List ID",
                name: "dncListId",
                type: "string",
                default: "",
                required: true,
                description: "Dnc list ID to which the items should be added. use the string `ID` returned by get /v1/dnc (e.g. \"vwjzrkzwaq\") \u2014 do not coerce it to a number.",
                placeholder: "e.g. vWjzRkZwAq",
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_addItemsToDncList"
                        ]
                    }
                }
            },
            {
                displayName: "Items",
                name: "items",
                type: "json",
                default: [],
                required: true,
                description: "Array of strings containing either emails or domains",
                placeholder: "e.g. example@example.com,domain.com",
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_addItemsToDncList"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_addItemsToDncList"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Dnc List ID",
                name: "dncListId",
                type: "string",
                default: "",
                required: true,
                description: "Enter dnc list ID",
                placeholder: "e.g. as5ax",
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_getDncById"
                        ]
                    }
                }
            },
            {
                displayName: "Type",
                name: "type",
                type: "options",
                default: "all",
                required: true,
                description: "Type of item",
                options: [
                    {
                        name: "All",
                        value: "all"
                    },
                    {
                        name: "Domain",
                        value: "domain"
                    },
                    {
                        name: "Email",
                        value: "email"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_getDncById"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_getDncById"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 1,
                        description: "Page number for pagination",
                        placeholder: "e.g. 1"
                    },
                    {
                        displayName: "Page Size",
                        name: "pageSize",
                        type: "number",
                        default: 25,
                        description: "Number of items per page. maximum pagesize is 100.",
                        placeholder: "e.g. 25"
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Search by item value",
                        placeholder: "e.g. Example List"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_getDncById"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Type",
                name: "type",
                type: "options",
                default: "all",
                required: true,
                description: "Type of item",
                options: [
                    {
                        name: "All",
                        value: "all"
                    },
                    {
                        name: "Domain",
                        value: "domain"
                    },
                    {
                        name: "Email",
                        value: "email"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_getDncListWithItem"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_getDncListWithItem"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 1,
                        description: "Page number for pagination",
                        placeholder: "e.g. 1"
                    },
                    {
                        displayName: "Page Size",
                        name: "pageSize",
                        type: "number",
                        default: 25,
                        description: "Number of items per page. maximum pagesize is 100.",
                        placeholder: "e.g. 25"
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Search by item value",
                        placeholder: "e.g. Example List"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_getDncListWithItem"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sort",
                name: "sort",
                type: "options",
                default: "ASC",
                required: true,
                description: "Sort order for the results",
                placeholder: "e.g. DESC",
                options: [
                    {
                        name: "ASC",
                        value: "ASC"
                    },
                    {
                        name: "DESC",
                        value: "DESC"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_getDncLists"
                        ]
                    }
                }
            },
            {
                displayName: "Sort By",
                name: "sortBy",
                type: "options",
                default: "total",
                required: true,
                description: "Field to sort the results by. ex: lastupdatedat, total email and domain count.",
                placeholder: "e.g. updatedAt",
                options: [
                    {
                        name: "Total",
                        value: "total"
                    },
                    {
                        name: "UpdatedAt",
                        value: "updatedAt"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_getDncLists"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_getDncLists"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 1,
                        description: "Page number for pagination",
                        placeholder: "e.g. 1"
                    },
                    {
                        displayName: "Page Size",
                        name: "pageSize",
                        type: "number",
                        default: 25,
                        description: "Number of items per page. maximum pagesize is 100.",
                        placeholder: "e.g. 25"
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Search by list name",
                        placeholder: "e.g. Example List"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "doNotContact"
                        ],
                        operation: [
                            "DncController_getDncLists"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ]
                    }
                },
                default: "EmailAccountController_addEmailAccounts",
                options: [
                    {
                        name: "Check Email Account Connection Status",
                        value: "EmailAccountController_getEmailAccountsConnectStatus",
                        action: "Check email account connection status",
                        description: "Check the progress of an email-account connection or reconnection and retrieve its CSV result link. email accounts."
                    },
                    {
                        name: "Connect An SMTP/IMAP",
                        value: "EmailAccountController_connectEmailAccount",
                        action: "Connect SMTP imap email account",
                        description: "Connect an SMTP/imap sending account. the account is saved even if verification fails; inspect smtpconnection and imapconnection in the response. status 0 means unverified; 1 means connected. emailserviceprovider is lowercase (zoho, gmail, yahoo, sendgrid, mailgun, ses, or outlook); provide fromname, fromfirstname, and fromlastname. email accounts."
                    },
                    {
                        name: "Connect New Sending Email Accounts SMTP And IMAP",
                        value: "EmailAccountController_addEmailAccounts",
                        action: "Connect new sending email accounts SMTP and imap",
                        description: "Start an asynchronous email-account connection. the response includes a requestid; poll get /v1/email-accounts/connect/status/{requestid} for progress and the resulting CSV link. email accounts."
                    },
                    {
                        name: "List Sending",
                        value: "EmailAccountController_getEmailAccounts",
                        action: "List sending email accounts",
                        description: "List email accounts matching the supplied filters and pagination settings"
                    },
                    {
                        name: "Reconnect Existing Email Accounts.",
                        value: "EmailAccountController_reconnectEmailAccounts",
                        action: "Reconnect existing email accounts",
                        description: "Start an asynchronous reconnection of email accounts. use the returned requestid with get /v1/email-accounts/connect/status/{requestid} to check progress and retrieve the CSV link."
                    },
                    {
                        name: "Update Sending Accounts In Bulk",
                        value: "EmailAccountController_updateEmailAccounts",
                        action: "Update sending accounts in bulk email accounts",
                        description: "Update settings for the listed email accounts, including sender details, quotas, signatures, sending intervals, ramp-up, or client assignment"
                    }
                ]
            },
            {
                displayName: "Body JSON",
                name: "bodyJson",
                type: "json",
                default: [],
                required: true,
                description: "Raw request body",
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_addEmailAccounts"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_addEmailAccounts"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Email Service Provider",
                name: "emailServiceProvider",
                type: "options",
                default: "gmail",
                required: true,
                description: "Email service provider identifier. accepted values: gmail, microsoft, gsuite, o365, yahoo, zoho, godaddy, yandex, sendgrid, other.",
                placeholder: "e.g. zoho",
                options: [
                    {
                        name: "Gmail",
                        value: "gmail"
                    },
                    {
                        name: "Godaddy",
                        value: "godaddy"
                    },
                    {
                        name: "Gsuite",
                        value: "gsuite"
                    },
                    {
                        name: "Microsoft",
                        value: "microsoft"
                    },
                    {
                        name: "O365",
                        value: "o365"
                    },
                    {
                        name: "Other",
                        value: "other"
                    },
                    {
                        name: "Sendgrid",
                        value: "sendgrid"
                    },
                    {
                        name: "Yahoo",
                        value: "yahoo"
                    },
                    {
                        name: "Yandex",
                        value: "yandex"
                    },
                    {
                        name: "Zoho",
                        value: "zoho"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_connectEmailAccount"
                        ]
                    }
                }
            },
            {
                displayName: "From First Name",
                name: "fromFirstName",
                type: "string",
                default: "",
                required: true,
                description: "Sender's first name",
                placeholder: "e.g. Sarah",
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_connectEmailAccount"
                        ]
                    }
                }
            },
            {
                displayName: "From Last Name",
                name: "fromLastName",
                type: "string",
                default: "",
                required: true,
                description: "Sender's last name",
                placeholder: "e.g. Chen",
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_connectEmailAccount"
                        ]
                    }
                }
            },
            {
                displayName: "From Name",
                name: "fromName",
                type: "string",
                default: "",
                required: true,
                description: "Full display name shown as the sender",
                placeholder: "e.g. Sarah Chen",
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_connectEmailAccount"
                        ]
                    }
                }
            },
            {
                displayName: "Payload",
                name: "payload",
                type: "collection",
                default: {
                    imap: {
                        emailAddress: "",
                        encryption: "TLS",
                        host: "",
                        password: "",
                        port: 0
                    },
                    smtp: {
                        emailAddress: "",
                        encryption: "TLS",
                        host: "",
                        password: "",
                        port: 0,
                        userName: ""
                    }
                },
                placeholder: "Add Field",
                options: [
                    {
                        displayName: "Imap",
                        name: "imap",
                        type: "collection",
                        default: {
                            emailAddress: "",
                            encryption: "TLS",
                            host: "",
                            password: "",
                            port: 0
                        },
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Email Address",
                                name: "emailAddress",
                                type: "string",
                                default: "",
                                description: "Email address used for imap authentication",
                                placeholder: "e.g. sarah.chen@outreach.io"
                            },
                            {
                                displayName: "Encryption",
                                name: "encryption",
                                type: "options",
                                default: "TLS",
                                description: "Imap connection encryption",
                                placeholder: "e.g. SSL",
                                options: [
                                    {
                                        name: "SSL",
                                        value: "SSL"
                                    },
                                    {
                                        name: "TLS",
                                        value: "TLS"
                                    }
                                ]
                            },
                            {
                                displayName: "Host",
                                name: "host",
                                type: "string",
                                default: "",
                                description: "Imap hostname",
                                placeholder: "e.g. imap.zoho.com"
                            },
                            {
                                displayName: "Password",
                                name: "password",
                                type: "string",
                                default: "",
                                description: "Imap password or app-specific password",
                                placeholder: "e.g. AppPassword123",
                                typeOptions: {
                                    password: true
                                }
                            },
                            {
                                displayName: "Port",
                                name: "port",
                                type: "number",
                                default: 0,
                                description: "Imap port (1\u201365535)",
                                placeholder: "e.g. 993",
                                typeOptions: {
                                    minValue: 1,
                                    maxValue: 65535
                                }
                            }
                        ],
                        description: "Imap connection settings"
                    },
                    {
                        displayName: "SMTP",
                        name: "smtp",
                        type: "collection",
                        default: {
                            emailAddress: "",
                            encryption: "TLS",
                            host: "",
                            password: "",
                            port: 0,
                            userName: ""
                        },
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Email Address",
                                name: "emailAddress",
                                type: "string",
                                default: "",
                                description: "Email address used for SMTP authentication",
                                placeholder: "e.g. sarah.chen@outreach.io"
                            },
                            {
                                displayName: "Encryption",
                                name: "encryption",
                                type: "options",
                                default: "TLS",
                                description: "SMTP connection encryption",
                                placeholder: "e.g. TLS",
                                options: [
                                    {
                                        name: "SSL",
                                        value: "SSL"
                                    },
                                    {
                                        name: "TLS",
                                        value: "TLS"
                                    }
                                ]
                            },
                            {
                                displayName: "Host",
                                name: "host",
                                type: "string",
                                default: "",
                                description: "SMTP hostname",
                                placeholder: "e.g. smtp.zoho.com"
                            },
                            {
                                displayName: "Password",
                                name: "password",
                                type: "string",
                                default: "",
                                description: "SMTP password or app-specific password",
                                placeholder: "e.g. AppPassword123",
                                typeOptions: {
                                    password: true
                                }
                            },
                            {
                                displayName: "Port",
                                name: "port",
                                type: "number",
                                default: 0,
                                description: "SMTP port (1\u201365535)",
                                placeholder: "e.g. 587",
                                typeOptions: {
                                    minValue: 1,
                                    maxValue: 65535
                                }
                            },
                            {
                                displayName: "User Name",
                                name: "userName",
                                type: "string",
                                default: "",
                                description: "SMTP username \u2014 usually the same as the email address",
                                placeholder: "e.g. sarah.chen@outreach.io"
                            }
                        ],
                        description: "SMTP connection settings"
                    }
                ],
                required: true,
                description: "SMTP + imap connection settings",
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_connectEmailAccount"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_connectEmailAccount"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_getEmailAccounts"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Added By",
                        name: "addedBy",
                        type: "json",
                        default: [],
                        description: "List of user IDs who added the email accounts",
                        placeholder: "e.g. JA5YdAr9wy"
                    },
                    {
                        displayName: "Client IDs",
                        name: "clientIds",
                        type: "json",
                        default: [],
                        description: "Array of email account IDs associated with the user",
                        placeholder: "e.g. JA5YdAr9wy"
                    },
                    {
                        displayName: "Email Service Provider",
                        name: "emailServiceProvider",
                        type: "json",
                        default: [],
                        description: "List of email service providers associated with the email accounts",
                        placeholder: "e.g. gsuite,microsoft"
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 1,
                        description: "Page number for pagination. defaults to the first page.",
                        placeholder: "e.g. 1"
                    },
                    {
                        displayName: "Page Size",
                        name: "pageSize",
                        type: "number",
                        default: 25,
                        description: "Number of items per page. maximum pagesize is 100.",
                        placeholder: "e.g. 25"
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Search term for filtering email accounts based on email or firstname or lastname",
                        placeholder: "e.g. john.doe@example.com"
                    },
                    {
                        displayName: "Sequence IDs",
                        name: "sequenceIds",
                        type: "json",
                        default: [],
                        description: "List of sequence IDs to filter the email accounts",
                        placeholder: "e.g. JA5YdAr9wy"
                    },
                    {
                        displayName: "Sort",
                        name: "sort",
                        type: "options",
                        default: "ASC",
                        description: "Sort order for the results, either ascending or descending",
                        placeholder: "e.g. DESC",
                        options: [
                            {
                                name: "ASC",
                                value: "ASC"
                            },
                            {
                                name: "DESC",
                                value: "DESC"
                            }
                        ]
                    },
                    {
                        displayName: "Sort By Key",
                        name: "sortByKey",
                        type: "options",
                        default: "created-date",
                        description: "Sort key to order the email accounts list. defaults to creation date.",
                        placeholder: "e.g. health-score",
                        options: [
                            {
                                name: "ClientFirstName",
                                value: "clientFirstName"
                            },
                            {
                                name: "Created Date",
                                value: "created-date"
                            },
                            {
                                name: "Health Score",
                                value: "health-score"
                            },
                            {
                                name: "Remaining Quota",
                                value: "remaining-quota"
                            }
                        ]
                    },
                    {
                        displayName: "Status",
                        name: "status",
                        type: "number",
                        default: 0,
                        description: "Filter email accounts by status. 0 for inactive, 1 for active, 2 for suspended.",
                        placeholder: "e.g. 1"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_getEmailAccounts"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Request ID",
                name: "requestId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_getEmailAccountsConnectStatus"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_getEmailAccountsConnectStatus"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Email Account IDs",
                name: "emailAccountIds",
                type: "json",
                default: [],
                required: true,
                description: "Array of email account IDs to be toggled on",
                placeholder: "e.g. 1Gz3xlNwr9,ajzR8xpPAq,vXwAZr6P8q",
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_reconnectEmailAccounts"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_reconnectEmailAccounts"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Email Account IDs",
                name: "emailAccountIds",
                type: "json",
                default: [],
                required: true,
                description: "Array of email account IDs associated with the user",
                placeholder: "e.g. Z6zxEXJwAk,D6zxEXJwAk",
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_updateEmailAccounts"
                        ]
                    }
                }
            },
            {
                displayName: "Sender First Name",
                name: "senderFirstName",
                type: "string",
                default: "",
                required: true,
                description: "First name of the sender (required)",
                placeholder: "e.g. John",
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_updateEmailAccounts"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_updateEmailAccounts"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Bcc",
                        name: "bcc",
                        type: "string",
                        default: "",
                        description: "Bcc email addresses (comma-separated if multiple)",
                        placeholder: "e.g. bcc@example.com"
                    },
                    {
                        displayName: "Client ID",
                        name: "clientId",
                        type: "number",
                        default: 0,
                        description: "Client ID associated with the user",
                        placeholder: "e.g. JA5YdAr9wy"
                    },
                    {
                        displayName: "Daily Quota",
                        name: "dailyQuota",
                        type: "number",
                        default: 0,
                        description: "Daily sending quota for the email account",
                        placeholder: "e.g. 500"
                    },
                    {
                        displayName: "Ramp Up Initial Sending Limit",
                        name: "rampUpInitialSendingLimit",
                        type: "number",
                        default: 0,
                        description: "Initial sending limit for ramp-up",
                        placeholder: "e.g. 100"
                    },
                    {
                        displayName: "Ramp Up Percent",
                        name: "rampUpPercent",
                        type: "number",
                        default: 0,
                        description: "Ramp-up percentage increase per interval",
                        placeholder: "e.g. 10"
                    },
                    {
                        displayName: "Sender Last Name",
                        name: "senderLastName",
                        type: "string",
                        default: "",
                        description: "Last name of the sender",
                        placeholder: "e.g. Doe"
                    },
                    {
                        displayName: "Sending Interval Max",
                        name: "sendingIntervalMax",
                        type: "number",
                        default: 0,
                        description: "Maximum interval (in seconds) between email sends",
                        placeholder: "e.g. 120"
                    },
                    {
                        displayName: "Sending Interval Min",
                        name: "sendingIntervalMin",
                        type: "number",
                        default: 0,
                        description: "Minimum interval (in seconds) between email sends",
                        placeholder: "e.g. 60"
                    },
                    {
                        displayName: "Signature HTML",
                        name: "signatureHtml",
                        type: "string",
                        default: "",
                        description: "HTML signature for the email",
                        placeholder: "e.g. <p>Best regards,<br>John Doe</p>"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "emailAccounts"
                        ],
                        operation: [
                            "EmailAccountController_updateEmailAccounts"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ]
                    }
                },
                default: "LeadFinderController_aiChat",
                options: [
                    {
                        name: "Add Leads To A Sequence",
                        value: "LeadFinderController_bulkAddLeadsToSequence",
                        action: "Add leads to a sequence enrichment",
                        description: "Add up to 10,000 lead-finder leads to a sequence step, optionally assigning existing or new tags. enrichment."
                    },
                    {
                        name: "Enrich Companies",
                        value: "LeadFinderController_enrichCompanies",
                        action: "Enrich companies enrichment",
                        description: "Start company enrichment for up to 100 companies using domains, website URLs, company IDs, or linkedin URLs. an optional webhook can receive completion results."
                    },
                    {
                        name: "Enrich People",
                        value: "LeadFinderController_enrichLeads",
                        action: "Enrich people enrichment",
                        description: "Start contact enrichment for up to 100 people using linkedin URLs, lead IDs, or name and company details. an optional webhook can receive completion results."
                    },
                    {
                        name: "Get API Rate Limits",
                        value: "LeadFinderController_getRateLimitStatus",
                        action: "Get API rate limits enrichment",
                        description: "Return the current API rate-limit status for the account. enrichment."
                    },
                    {
                        name: "Get Credit Balance And Usage",
                        value: "LeadFinderController_getCreditDetails",
                        action: "Get credit balance and usage enrichment",
                        description: "Return the account credit balance and usage summary. enrichment."
                    },
                    {
                        name: "Get Enrichment Job Status",
                        value: "LeadFinderController_getEnrichmentStatus",
                        action: "Get enrichment job status",
                        description: "Return the status of a people or company enrichment job"
                    },
                    {
                        name: "Get Enrichment Results",
                        value: "LeadFinderController_getEnrichmentResult",
                        action: "Get enrichment results",
                        description: "Return the results of a completed enrichment job"
                    },
                    {
                        name: "List Lead Search Filters",
                        value: "LeadFinderController_getLeadFinderFilters",
                        action: "List lead search filters enrichment",
                        description: "Return the lead search filters available on the current plan. enrichment."
                    },
                    {
                        name: "Search Companies",
                        value: "LeadFinderController_searchCompanies",
                        action: "Search companies enrichment",
                        description: "Search for companies using the supplied company, location, industry, funding, size, and other filters. enrichment."
                    },
                    {
                        name: "Search Leads With AI",
                        value: "LeadFinderController_aiChat",
                        action: "Search leads with ai enrichment",
                        description: "Run a natural-language search and return the assistant response with up to 25 matching leads or companies when results are found. enrichment."
                    },
                    {
                        name: "Search People",
                        value: "LeadFinderController_searchLeads",
                        action: "Search people enrichment",
                        description: "Search for people using the supplied lead, role, location, and company filters. enrichment."
                    }
                ]
            },
            {
                displayName: "Query",
                name: "query",
                type: "string",
                default: "",
                required: true,
                description: "Natural-language query describing the leads or companies to find",
                placeholder: "e.g. Find fintech companies with 51-200 employees",
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_aiChat"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_aiChat"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Conversation ID",
                        name: "conversationId",
                        type: "string",
                        default: "",
                        description: "Existing conversation ID to continue a previous ai chat session"
                    },
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_aiChat"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Lead IDs",
                name: "leadIds",
                type: "json",
                default: [],
                required: true,
                description: "Lead IDs to add to the sequence (max 10000)",
                placeholder: "e.g. 12345,67890",
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_bulkAddLeadsToSequence"
                        ]
                    }
                }
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                placeholder: "e.g. bwOLEx4l8G",
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_bulkAddLeadsToSequence"
                        ]
                    }
                }
            },
            {
                displayName: "Step ID",
                name: "stepId",
                type: "string",
                default: "",
                required: true,
                description: "Step ID of the sequence",
                placeholder: "e.g. 2dP27N0gZ4",
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_bulkAddLeadsToSequence"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_bulkAddLeadsToSequence"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "New Tags",
                        name: "newTags",
                        type: "json",
                        default: [],
                        description: "New tag names to create and assign to the leads",
                        placeholder: "e.g. Tag1,Tag2"
                    },
                    {
                        displayName: "Tag IDs",
                        name: "tagIds",
                        type: "json",
                        default: [],
                        description: "Existing tag IDs to assign to the leads",
                        placeholder: "e.g. VMw56r9jPb"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_bulkAddLeadsToSequence"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_enrichCompanies"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Company Domain",
                        name: "company_domain",
                        type: "json",
                        default: [],
                        description: "Company domains to enrich (max 100)",
                        placeholder: "e.g. acme.com"
                    },
                    {
                        displayName: "Company ID",
                        name: "company_id",
                        type: "json",
                        default: [],
                        description: "Company IDs to enrich (max 100)",
                        placeholder: "e.g. 12345"
                    },
                    {
                        displayName: "Company Website",
                        name: "company_website",
                        type: "json",
                        default: [],
                        description: "Company website URLs to enrich (max 100)",
                        placeholder: "e.g. https://acme.com"
                    },
                    {
                        displayName: "Linkedin URL",
                        name: "linkedin_url",
                        type: "json",
                        default: [],
                        description: "Linkedin company URLs to enrich (max 100)",
                        placeholder: "e.g. https://linkedin.com/company/acme"
                    },
                    {
                        displayName: "Webhook URL",
                        name: "webhook_url",
                        type: "string",
                        default: "",
                        description: "Webhook URL to call when enrichment completes"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_enrichCompanies"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_enrichLeads"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Email Type",
                        name: "email_type",
                        type: "options",
                        default: "work_email",
                        description: "Email type to reveal. with reveal_phone=false, returns the selected email; with true, work_email returns work email and phone, while personal_email returns both email types and phone. not allowed when profile_only is true.",
                        options: [
                            {
                                name: "Personal Email",
                                value: "personal_email"
                            },
                            {
                                name: "Work Email",
                                value: "work_email"
                            }
                        ]
                    },
                    {
                        displayName: "Full Name With Company",
                        name: "full_name_with_company",
                        type: "json",
                        default: [],
                        description: "Full name + company combinations to enrich (max 100)"
                    },
                    {
                        displayName: "Lead ID",
                        name: "lead_id",
                        type: "json",
                        default: [],
                        description: "Lead IDs to enrich (max 100)",
                        placeholder: "e.g. 12345,67890"
                    },
                    {
                        displayName: "Linkedin URL",
                        name: "linkedin_url",
                        type: "json",
                        default: [],
                        description: "Linkedin profile URLs to enrich (max 100)",
                        placeholder: "e.g. https://linkedin.com/in/johndoe"
                    },
                    {
                        displayName: "Profile Only",
                        name: "profile_only",
                        type: "boolean",
                        default: false,
                        description: "Whether save profile data only (name, job title, company, location, linkedin) with no contact lookup, at a reduced credit cost. mutually exclusive with `email_type` and `reveal_phone: true` \u2014 sending either alongside it is rejected."
                    },
                    {
                        displayName: "Reveal Phone",
                        name: "reveal_phone",
                        type: "boolean",
                        default: false,
                        description: "Whether to reveal phone numbers"
                    },
                    {
                        displayName: "Webhook URL",
                        name: "webhook_url",
                        type: "string",
                        default: "",
                        description: "Webhook URL to call when enrichment completes"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_enrichLeads"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_getCreditDetails"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Request ID",
                name: "requestId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_getEnrichmentResult"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_getEnrichmentResult"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Request ID",
                name: "requestId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_getEnrichmentStatus"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_getEnrichmentStatus"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_getLeadFinderFilters"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_getRateLimitStatus"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_searchCompanies"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Active Job Postings Count",
                        name: "active_job_postings_count",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Active Job Postings Title",
                        name: "active_job_postings_title",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Base Salary",
                        name: "base_salary",
                        type: "json",
                        default: [],
                        placeholder: "e.g. [object Object]"
                    },
                    {
                        displayName: "Company Annual Revenue",
                        name: "company_annual_revenue",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Company Domain",
                        name: "company_domain",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Company Founded Year",
                        name: "company_founded_year",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Company Funding Amount",
                        name: "company_funding_amount",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Company Funding Date",
                        name: "company_funding_date",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Company Hq Location",
                        name: "company_hq_location",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Exact Match",
                                name: "exactMatch",
                                type: "options",
                                default: 0,
                                options: [
                                    {
                                        name: "0",
                                        value: 0
                                    },
                                    {
                                        name: "1",
                                        value: 1
                                    }
                                ]
                            },
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Company Industry",
                        name: "company_industry",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Company Name",
                        name: "company_name",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Exact Match",
                                name: "exactMatch",
                                type: "options",
                                default: 0,
                                options: [
                                    {
                                        name: "0",
                                        value: 0
                                    },
                                    {
                                        name: "1",
                                        value: 1
                                    }
                                ]
                            },
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Company Size",
                        name: "company_size",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Employee Count Department",
                        name: "employee_count_department",
                        type: "json",
                        default: [],
                        placeholder: "e.g. [object Object],[object Object]"
                    },
                    {
                        displayName: "Employee Count Seniority",
                        name: "employee_count_seniority",
                        type: "json",
                        default: [],
                        placeholder: "e.g. [object Object],[object Object]"
                    },
                    {
                        displayName: "Employee Reviews Aggregate Score",
                        name: "employee_reviews_aggregate_score",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Employee Reviews Business Outlook",
                        name: "employee_reviews_business_outlook",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Employee Reviews Career Opportunities",
                        name: "employee_reviews_career_opportunities",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Employee Reviews Ceo Approval",
                        name: "employee_reviews_ceo_approval",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Employee Reviews Culture Values",
                        name: "employee_reviews_culture_values",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Employee Reviews Recommend",
                        name: "employee_reviews_recommend",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Employee Reviews Total Count",
                        name: "employee_reviews_total_count",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Employee Reviews Work Life Balance",
                        name: "employee_reviews_work_life_balance",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Followers Count Linkedin",
                        name: "followers_count_linkedin",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Funding Rounds Name",
                        name: "funding_rounds_name",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Is B2b",
                        name: "is_b2b",
                        type: "boolean",
                        default: false,
                        description: "Whether to enable is b2b",
                        placeholder: "e.g. true"
                    },
                    {
                        displayName: "Keywords",
                        name: "keywords",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Last Funding Round Amount Raised",
                        name: "last_funding_round_amount_raised",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Last Funding Round Announced Date",
                        name: "last_funding_round_announced_date",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Last Funding Round Name",
                        name: "last_funding_round_name",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Linkedin URL",
                        name: "linkedin_url",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Exact Match",
                                name: "exactMatch",
                                type: "options",
                                default: 0,
                                options: [
                                    {
                                        name: "0",
                                        value: 0
                                    },
                                    {
                                        name: "1",
                                        value: 1
                                    }
                                ]
                            },
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Look Alike Company Name",
                        name: "look_alike_company_name",
                        type: "json",
                        default: [],
                        placeholder: "e.g. [object Object],[object Object]"
                    },
                    {
                        displayName: "Naics Codes",
                        name: "naics_codes",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "News IDs",
                        name: "newsIds",
                        type: "json",
                        default: [],
                        placeholder: "e.g. 2,4,7"
                    },
                    {
                        displayName: "Ownership Status",
                        name: "ownership_status",
                        type: "options",
                        default: "Private",
                        placeholder: "e.g. Private",
                        options: [
                            {
                                name: "Other",
                                value: "Other"
                            },
                            {
                                name: "Private",
                                value: "Private"
                            },
                            {
                                name: "Public",
                                value: "Public"
                            }
                        ]
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 1,
                        placeholder: "e.g. 1",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 400
                        }
                    },
                    {
                        displayName: "Product Reviews Aggregate Score",
                        name: "product_reviews_aggregate_score",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Product Reviews Count",
                        name: "product_reviews_count",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Product Reviews Score Change",
                        name: "product_reviews_score_change",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Direction",
                                name: "direction",
                                type: "options",
                                default: "up",
                                options: [
                                    {
                                        name: "Any",
                                        value: "any"
                                    },
                                    {
                                        name: "Down",
                                        value: "down"
                                    },
                                    {
                                        name: "Up",
                                        value: "up"
                                    }
                                ]
                            },
                            {
                                displayName: "Duration",
                                name: "duration",
                                type: "options",
                                default: "current",
                                options: [
                                    {
                                        name: "Current",
                                        value: "current"
                                    },
                                    {
                                        name: "Monthly",
                                        value: "monthly"
                                    },
                                    {
                                        name: "Quarterly",
                                        value: "quarterly"
                                    },
                                    {
                                        name: "Yearly",
                                        value: "yearly"
                                    }
                                ]
                            },
                            {
                                displayName: "Points Range",
                                name: "points_range",
                                type: "collection",
                                default: {},
                                placeholder: "Add Field",
                                options: [
                                    {
                                        displayName: "Max",
                                        name: "max",
                                        type: "string",
                                        default: ""
                                    },
                                    {
                                        displayName: "Min",
                                        name: "min",
                                        type: "string",
                                        default: ""
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        displayName: "Rank Global",
                        name: "rank_global",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Sic Codes",
                        name: "sic_codes",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Signal IDs",
                        name: "signalIds",
                        type: "json",
                        default: [],
                        placeholder: "e.g. 2,3,4"
                    },
                    {
                        displayName: "Social URLs",
                        name: "social_urls",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Technologies Used",
                        name: "technologies_used",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Total Website Visits Monthly",
                        name: "total_website_visits_monthly",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Type",
                        name: "type",
                        type: "options",
                        default: "Privately Held",
                        placeholder: "e.g. Privately Held",
                        options: [
                            {
                                name: "Educational",
                                value: "Educational"
                            },
                            {
                                name: "Government Agency",
                                value: "Government Agency"
                            },
                            {
                                name: "Nonprofit",
                                value: "Nonprofit"
                            },
                            {
                                name: "Partnership",
                                value: "Partnership"
                            },
                            {
                                name: "Privately Held",
                                value: "Privately Held"
                            },
                            {
                                name: "Public Company",
                                value: "Public Company"
                            },
                            {
                                name: "Self Employed",
                                value: "Self-Employed"
                            },
                            {
                                name: "Self Owned",
                                value: "Self-Owned"
                            }
                        ]
                    },
                    {
                        displayName: "Visits Breakdown By Country",
                        name: "visits_breakdown_by_country",
                        type: "json",
                        default: [],
                        placeholder: "e.g. [object Object]"
                    },
                    {
                        displayName: "Visits Breakdown By Gender",
                        name: "visits_breakdown_by_gender",
                        type: "json",
                        default: [],
                        placeholder: "e.g. [object Object]"
                    },
                    {
                        displayName: "Website",
                        name: "website",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_searchCompanies"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_searchLeads"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Certifications",
                        name: "certifications",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Company Annual Revenue",
                        name: "company_annual_revenue",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Company Bootstraped",
                        name: "company_bootstraped",
                        type: "boolean",
                        default: false,
                        description: "Whether to enable company bootstraped",
                        placeholder: "e.g. true"
                    },
                    {
                        displayName: "Company Domain",
                        name: "company_domain",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Company Founded Year",
                        name: "company_founded_year",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Company Funding Amount",
                        name: "company_funding_amount",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Company Funding Date",
                        name: "company_funding_date",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Company Hq Location",
                        name: "company_hq_location",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Exact Match",
                                name: "exactMatch",
                                type: "options",
                                default: 0,
                                options: [
                                    {
                                        name: "0",
                                        value: 0
                                    },
                                    {
                                        name: "1",
                                        value: 1
                                    }
                                ]
                            },
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Company Industry",
                        name: "company_industry",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Company Name",
                        name: "company_name",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Exact Match",
                                name: "exactMatch",
                                type: "options",
                                default: 0,
                                options: [
                                    {
                                        name: "0",
                                        value: 0
                                    },
                                    {
                                        name: "1",
                                        value: 1
                                    }
                                ]
                            },
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Should Search Past",
                                name: "shouldSearchPast",
                                type: "number",
                                default: 0
                            }
                        ]
                    },
                    {
                        displayName: "Company Size",
                        name: "company_size",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Current Company Experience",
                        name: "current_company_experience",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Current Role Tenure",
                        name: "current_role_tenure",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Decision Maker",
                        name: "decision_maker",
                        type: "boolean",
                        default: false,
                        description: "Whether to enable decision maker",
                        placeholder: "e.g. true"
                    },
                    {
                        displayName: "Department",
                        name: "department",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Education Degrees",
                        name: "education_degrees",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Full Name",
                        name: "full_name",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Exact Match",
                                name: "exactMatch",
                                type: "options",
                                default: 0,
                                options: [
                                    {
                                        name: "0",
                                        value: 0
                                    },
                                    {
                                        name: "1",
                                        value: 1
                                    }
                                ]
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Is B2b",
                        name: "is_b2b",
                        type: "boolean",
                        default: false,
                        description: "Whether to enable is b2b",
                        placeholder: "e.g. true"
                    },
                    {
                        displayName: "Job Title",
                        name: "job_title",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Exact Match",
                                name: "exactMatch",
                                type: "options",
                                default: 0,
                                options: [
                                    {
                                        name: "0",
                                        value: 0
                                    },
                                    {
                                        name: "1",
                                        value: 1
                                    }
                                ]
                            },
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Should Search Past",
                                name: "shouldSearchPast",
                                type: "number",
                                default: 0
                            }
                        ]
                    },
                    {
                        displayName: "Jobs Changed Within",
                        name: "jobs_changed_within",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Keywords",
                        name: "keywords",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Limit Per Company",
                        name: "limitPerCompany",
                        type: "number",
                        default: 0,
                        placeholder: "e.g. 5",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 10000
                        }
                    },
                    {
                        displayName: "Linkedin Connection Count",
                        name: "linkedin_connection_count",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Linkedin Followers Count",
                        name: "linkedin_followers_count",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Linkedin URL",
                        name: "linkedin_url",
                        type: "json",
                        default: [],
                        placeholder: "e.g. https://linkedin.com/in/example-profile"
                    },
                    {
                        displayName: "Location",
                        name: "location",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Exact Match",
                                name: "exactMatch",
                                type: "options",
                                default: 0,
                                options: [
                                    {
                                        name: "0",
                                        value: 0
                                    },
                                    {
                                        name: "1",
                                        value: 1
                                    }
                                ]
                            },
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Management Level",
                        name: "management_level",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 1,
                        placeholder: "e.g. 1",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 400
                        }
                    },
                    {
                        displayName: "Projected Base Salary",
                        name: "projected_base_salary",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    },
                    {
                        displayName: "Skills",
                        name: "skills",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Exact Match",
                                name: "exactMatch",
                                type: "options",
                                default: 0,
                                options: [
                                    {
                                        name: "0",
                                        value: 0
                                    },
                                    {
                                        name: "1",
                                        value: 1
                                    }
                                ]
                            },
                            {
                                displayName: "Excludes",
                                name: "excludes",
                                type: "json",
                                default: []
                            },
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Social Link",
                        name: "social_link",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Includes",
                                name: "includes",
                                type: "json",
                                default: []
                            }
                        ]
                    },
                    {
                        displayName: "Total Experience",
                        name: "total_experience",
                        type: "collection",
                        default: {},
                        placeholder: "Add Field",
                        options: [
                            {
                                displayName: "Max",
                                name: "max",
                                type: "string",
                                default: ""
                            },
                            {
                                displayName: "Min",
                                name: "min",
                                type: "string",
                                default: ""
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "enrichment"
                        ],
                        operation: [
                            "LeadFinderController_searchLeads"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "fields"
                        ]
                    }
                },
                default: "FieldController_createField",
                options: [
                    {
                        name: "Create A Custom",
                        value: "FieldController_createField",
                        action: "Create custom field",
                        description: "Creates a new custom field that can be used to store additional information on prospect profiles. supported field types are text, number, long text, currency, date, and dropdown."
                    },
                    {
                        name: "List All",
                        value: "FieldController_getFields",
                        action: "List all fields",
                        description: "List account fields. set systemfields=true to include built-in fields; omit it or set false to return custom fields only. each field includes its resolved tags."
                    },
                    {
                        name: "Update A Custom",
                        value: "FieldController_updateField",
                        action: "Update custom field",
                        description: "Updates the label, fallback text, and/or tags of an existing custom field. only custom (non-system) fields can be updated. use the field ID returned by the get /fields endpoint."
                    }
                ]
            },
            {
                displayName: "Field Type",
                name: "fieldType",
                type: "options",
                default: "text",
                required: true,
                description: "The data type of the custom field. accepted values: text, number, date, long-text, dropdown, currency.",
                placeholder: "e.g. text",
                options: [
                    {
                        name: "Currency",
                        value: "currency"
                    },
                    {
                        name: "Date",
                        value: "date"
                    },
                    {
                        name: "Dropdown",
                        value: "dropdown"
                    },
                    {
                        name: "Long Text",
                        value: "long-text"
                    },
                    {
                        name: "Number",
                        value: "number"
                    },
                    {
                        name: "Text",
                        value: "text"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "fields"
                        ],
                        operation: [
                            "FieldController_createField"
                        ]
                    }
                }
            },
            {
                displayName: "Label",
                name: "label",
                type: "string",
                default: "",
                required: true,
                description: "The display name of the custom field. this label will appear in the prospect profile and throughout the application.",
                placeholder: "e.g. LinkedIn Profile URL",
                displayOptions: {
                    show: {
                        resource: [
                            "fields"
                        ],
                        operation: [
                            "FieldController_createField"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "fields"
                        ],
                        operation: [
                            "FieldController_createField"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Fallback Text",
                        name: "fallbackText",
                        type: "string",
                        default: "",
                        description: "A default placeholder text to display when the field has no value for a prospect",
                        placeholder: "e.g. Not provided"
                    },
                    {
                        displayName: "Metadata",
                        name: "metadata",
                        type: "json",
                        default: {},
                        description: "Type-specific settings. dropdown fields require metadata.options with up to 50 values. currency fields require metadata.currencycode, an ISO 4217 code; supported codes include usd, eur, gbp, jpy, chf, cad, aud, cny, inr, sgd, hkd, nzd, krw, sek, aed, brl, mxn, zar, and sar. ignored for other field types.",
                        placeholder: "e.g. [object Object]"
                    },
                    {
                        displayName: "Tags",
                        name: "tags",
                        type: "json",
                        default: [],
                        description: "Freeform tags for finding this field later (separate namespace from prospect tags). never blocks create \u2014 resolved case-insensitively, existing tags reused.",
                        placeholder: "e.g. CRM,Sales"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "fields"
                        ],
                        operation: [
                            "FieldController_createField"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "System Fields",
                name: "systemFields",
                type: "options",
                default: "true",
                required: true,
                description: "Pass <code>true</code> to include built-in system fields (such as first name, last name, and email) along with your custom fields. pass <code>false</code> or omit this parameter to return only custom fields.",
                options: [
                    {
                        name: "False",
                        value: "false"
                    },
                    {
                        name: "True",
                        value: "true"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "fields"
                        ],
                        operation: [
                            "FieldController_getFields"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "fields"
                        ],
                        operation: [
                            "FieldController_getFields"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Field ID",
                name: "fieldId",
                type: "string",
                default: "",
                required: true,
                description: "Public custom-field ID returned by field read/list APIs",
                placeholder: "e.g. Vj9kLp2Q",
                displayOptions: {
                    show: {
                        resource: [
                            "fields"
                        ],
                        operation: [
                            "FieldController_updateField"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "fields"
                        ],
                        operation: [
                            "FieldController_updateField"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Fallback Text",
                        name: "fallbackText",
                        type: "string",
                        default: "",
                        description: "Updated placeholder text to display when the field has no value for a prospect",
                        placeholder: "e.g. Not available"
                    },
                    {
                        displayName: "Label",
                        name: "label",
                        type: "string",
                        default: "",
                        description: "The new display name for the custom field. this label will appear in the prospect profile and throughout the application.",
                        placeholder: "e.g. LinkedIn URL"
                    },
                    {
                        displayName: "Tags",
                        name: "tags",
                        type: "json",
                        default: [],
                        description: "Full replacement list for this field?s tags, separate from prospect tags. omit to keep current tags; pass [] to remove them. supplied tags are added or reused case-insensitively, and omitted current tags are removed.",
                        placeholder: "e.g. CRM,Sales"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "fields"
                        ],
                        operation: [
                            "FieldController_updateField"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "leadsToEmail"
                        ]
                    }
                },
                default: "LeadsToEmailController_createDomainsWorkflow",
                options: [
                    {
                        name: "Create A Domain Based Lead Workflow",
                        value: "LeadsToEmailController_createDomainsWorkflow",
                        action: "Create domain based lead workflow leads to email",
                        description: "Start a workflow that finds people at the supplied company domains, enriches them, and sends the results to your webhook. leads to email."
                    },
                    {
                        name: "Create A Leads To Email Workflow",
                        value: "LeadsToEmailController_createWorkflow",
                        action: "Create leads to email workflow leads to email",
                        description: "<Strong>content-type</strong>: <code>multipart/form-data</code> with a <code>file</code> field (the CSV) plus the text fields below. JSON-valued fields (<code>columnmapping</code>, <code>exportcolumns</code>, <code>filters</code>) must be sent as stringified JSON. leads to email."
                    }
                ]
            },
            {
                displayName: "Domains",
                name: "domains",
                type: "json",
                default: [],
                required: true,
                description: "Company domains to enrich. each entry is a bare domain/URL string or an object { domain, jobtitle?, department?, managementlevel?, decisionmaker? } whose criteria filter the people picked for that domain.",
                placeholder: "e.g. saleshandy.com,[object Object]",
                displayOptions: {
                    show: {
                        resource: [
                            "leadsToEmail"
                        ],
                        operation: [
                            "LeadsToEmailController_createDomainsWorkflow"
                        ]
                    }
                }
            },
            {
                displayName: "Webhook URL",
                name: "webhookUrl",
                type: "string",
                default: "",
                required: true,
                description: "Webhook URL that receives the final result once enrichment completes (single webhook with the enriched records)",
                placeholder: "e.g. https://example.com/webhooks/leads-to-email",
                displayOptions: {
                    show: {
                        resource: [
                            "leadsToEmail"
                        ],
                        operation: [
                            "LeadsToEmailController_createDomainsWorkflow"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "leadsToEmail"
                        ],
                        operation: [
                            "LeadsToEmailController_createDomainsWorkflow"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Export Columns",
                        name: "exportColumns",
                        type: "json",
                        default: [],
                        description: "Columns for the enriched output. defaults to the key-columns set when omitted or empty."
                    },
                    {
                        displayName: "Filters",
                        name: "filters",
                        type: "json",
                        default: {},
                        description: "Search filters and-combined onto every domain's people search \u2014 e.g. { \"location\": { \"includes\": [\"united states\"] } }"
                    },
                    {
                        displayName: "Leads Per Domain",
                        name: "leadsPerDomain",
                        type: "number",
                        default: 2,
                        description: "How many people to enrich per domain (top-ranked by lead score). defaults to 2 when omitted.",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 10
                        }
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "leadsToEmail"
                        ],
                        operation: [
                            "LeadsToEmailController_createDomainsWorkflow"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Column Mapping",
                name: "columnMapping",
                type: "string",
                default: "",
                required: true,
                placeholder: "e.g. {\"First Name\":\"firstName\",\"Company Domain\":\"domain\"}",
                displayOptions: {
                    show: {
                        resource: [
                            "leadsToEmail"
                        ],
                        operation: [
                            "LeadsToEmailController_createWorkflow"
                        ]
                    }
                }
            },
            {
                displayName: "File",
                name: "file",
                type: "string",
                default: "",
                required: true,
                description: "CSV file with the leads to enrich",
                displayOptions: {
                    show: {
                        resource: [
                            "leadsToEmail"
                        ],
                        operation: [
                            "LeadsToEmailController_createWorkflow"
                        ]
                    }
                }
            },
            {
                displayName: "Reveal Type",
                name: "revealType",
                type: "string",
                default: "",
                required: true,
                placeholder: "e.g. email",
                displayOptions: {
                    show: {
                        resource: [
                            "leadsToEmail"
                        ],
                        operation: [
                            "LeadsToEmailController_createWorkflow"
                        ]
                    }
                }
            },
            {
                displayName: "Total Records",
                name: "totalRecords",
                type: "string",
                default: "",
                required: true,
                placeholder: "e.g. 250",
                displayOptions: {
                    show: {
                        resource: [
                            "leadsToEmail"
                        ],
                        operation: [
                            "LeadsToEmailController_createWorkflow"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "leadsToEmail"
                        ],
                        operation: [
                            "LeadsToEmailController_createWorkflow"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Body Template ID",
                        name: "bodyTemplateId",
                        type: "string",
                        default: ""
                    },
                    {
                        displayName: "Export Columns",
                        name: "exportColumns",
                        type: "string",
                        default: "",
                        placeholder: "e.g. [\"firstName\",\"lastName\",\"email\"]"
                    },
                    {
                        displayName: "File Name",
                        name: "fileName",
                        type: "string",
                        default: "",
                        placeholder: "e.g. q3-leads.csv"
                    },
                    {
                        displayName: "Filters",
                        name: "filters",
                        type: "string",
                        default: ""
                    },
                    {
                        displayName: "Step Content",
                        name: "stepContent",
                        type: "string",
                        default: "",
                        description: "Stringified JSON array of caller-provided steps; skips ai compose",
                        placeholder: "e.g. [{\"stepNumber\":1,\"subject\":\"Hi {{First Name}}\",\"content\":\"<p>Intro\u2026</p>\"}]"
                    },
                    {
                        displayName: "Step Gap",
                        name: "stepGap",
                        type: "string",
                        default: "",
                        placeholder: "e.g. 3"
                    },
                    {
                        displayName: "Steps",
                        name: "steps",
                        type: "string",
                        default: "",
                        placeholder: "e.g. 3"
                    },
                    {
                        displayName: "Subject Template ID",
                        name: "subjectTemplateId",
                        type: "string",
                        default: ""
                    },
                    {
                        displayName: "Webhook URL",
                        name: "webhookUrl",
                        type: "string",
                        default: "",
                        placeholder: "e.g. https://example.com/webhooks/leads-to-email"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "leadsToEmail"
                        ],
                        operation: [
                            "LeadsToEmailController_createWorkflow"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "notes"
                        ]
                    }
                },
                default: "NoteController_createNote",
                options: [
                    {
                        name: "Create A",
                        value: "NoteController_createNote",
                        action: "Create note",
                        description: "Create a note for one prospect with prospectid or for several with prospectids. the response reports the result for each prospect."
                    },
                    {
                        name: "Update A",
                        value: "NoteController_updateNote",
                        action: "Update note",
                        description: "Updates the content or visibility of an existing note. only the fields you provide will be changed \u2014 any field you omit will remain unchanged. use the note ID returned when the note was created."
                    },
                    {
                        name: "Upload An Attachment For A",
                        value: "NoteController_uploadNoteAttachment",
                        action: "Upload attachment for a note",
                        description: "Upload a note attachment as multipart/form-data with one file field. files can be up to 20 mb; use the returned attachmentid when creating a note."
                    }
                ]
            },
            {
                displayName: "Content",
                name: "content",
                type: "string",
                default: "",
                required: true,
                description: "The content of the note. supports plain text and HTML.",
                placeholder: "e.g. Follow up with this prospect next week regarding the demo.",
                displayOptions: {
                    show: {
                        resource: [
                            "notes"
                        ],
                        operation: [
                            "NoteController_createNote"
                        ]
                    }
                }
            },
            {
                displayName: "Prospect IDs",
                name: "prospectIds",
                type: "json",
                default: [],
                required: true,
                description: "List of prospect IDs to attach the note to. pass a single-element array for a single prospect, or multiple IDs to attach the same note to several prospects in one request.",
                placeholder: "e.g. 8PvBmrB7P7",
                displayOptions: {
                    show: {
                        resource: [
                            "notes"
                        ],
                        operation: [
                            "NoteController_createNote"
                        ]
                    }
                }
            },
            {
                displayName: "Visibility",
                name: "visibility",
                type: "options",
                default: "1",
                required: true,
                description: "Who can see this note. use \"1\" for public (visible to all team members) or \"2\" for private (visible only to you).",
                placeholder: "e.g. 1",
                options: [
                    {
                        name: "1",
                        value: "1"
                    },
                    {
                        name: "2",
                        value: "2"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "notes"
                        ],
                        operation: [
                            "NoteController_createNote"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "notes"
                        ],
                        operation: [
                            "NoteController_createNote"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Attachment IDs",
                        name: "attachmentIds",
                        type: "json",
                        default: [],
                        description: "IDs of attachments to associate with this note. upload via post /v1/notes/attachments to obtain each attachmentid.",
                        placeholder: "e.g. Aw83gqRXvk,BV4yLpC2nX"
                    },
                    {
                        displayName: "Status",
                        name: "status",
                        type: "options",
                        default: "1",
                        description: "The status of the note. use \"0\" for draft or \"1\" for published. defaults to \"1\" (published) if not provided.",
                        placeholder: "e.g. 1",
                        options: [
                            {
                                name: "0",
                                value: "0"
                            },
                            {
                                name: "1",
                                value: "1"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "notes"
                        ],
                        operation: [
                            "NoteController_createNote"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Note ID",
                name: "noteId",
                type: "string",
                default: "",
                required: true,
                description: "Public note ID returned by create note or fetch notes",
                placeholder: "e.g. Nt7Yp4Za",
                displayOptions: {
                    show: {
                        resource: [
                            "notes"
                        ],
                        operation: [
                            "NoteController_updateNote"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "notes"
                        ],
                        operation: [
                            "NoteController_updateNote"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Content",
                        name: "content",
                        type: "string",
                        default: "",
                        description: "The updated content of the note. supports plain text.",
                        placeholder: "e.g. Prospect confirmed interest. Schedule a demo call for next Monday."
                    },
                    {
                        displayName: "Status",
                        name: "status",
                        type: "options",
                        default: "0",
                        description: "Updated status of the note. use \"0\" for draft or \"1\" for published.",
                        placeholder: "e.g. 1",
                        options: [
                            {
                                name: "0",
                                value: "0"
                            },
                            {
                                name: "1",
                                value: "1"
                            }
                        ]
                    },
                    {
                        displayName: "Visibility",
                        name: "visibility",
                        type: "options",
                        default: "1",
                        description: "Updated visibility setting. use \"1\" for public (visible to all team members) or \"2\" for private (visible only to you).",
                        placeholder: "e.g. 2",
                        options: [
                            {
                                name: "1",
                                value: "1"
                            },
                            {
                                name: "2",
                                value: "2"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "notes"
                        ],
                        operation: [
                            "NoteController_updateNote"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "File",
                name: "file",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "notes"
                        ],
                        operation: [
                            "NoteController_uploadNoteAttachment"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "notes"
                        ],
                        operation: [
                            "NoteController_uploadNoteAttachment"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ]
                    }
                },
                default: "ProspectController_getProspectAttributeById",
                options: [
                    {
                        name: "Assign Tags To",
                        value: "SequenceContactController_assignTagsToProspects",
                        action: "Assign tags to prospects",
                        description: "Create or reuse the supplied tags and assign them to prospects identified by ID or email"
                    },
                    {
                        name: "Check Email Verification Status",
                        value: "ProspectController_getProspectsVerificationStatus",
                        action: "Check email verification status prospects",
                        description: "Return email verification statuses for the submitted addresses. prospects."
                    },
                    {
                        name: "Check Prospect Import Status",
                        value: "ProspectController_getProspectImportStatus",
                        action: "Check prospect import status",
                        description: "Check the progress of an import started through either prospect-import endpoint. the response includes any import errors as a CSV link."
                    },
                    {
                        name: "Get A Contact Sequence History",
                        value: "SequenceContactController_getContactMinimalSequences",
                        action: "Get contact sequence history prospects",
                        description: "Return the sequence history for the specified contact. prospects."
                    },
                    {
                        name: "Get A Prospect Attribute Value",
                        value: "ProspectController_getProspectAttributeById",
                        action: "Get prospect attribute value",
                        description: "Retrieve the long-text field value for a prospect attribute using its attribute ID and, optionally, a prospect ID"
                    },
                    {
                        name: "Handle A Prospect Unsubscribe Request",
                        value: "SequenceContactController_unsubscribeProspects",
                        action: "Handle prospect unsubscribe request"
                    },
                    {
                        name: "Import",
                        value: "ProspectController_importProspects",
                        action: "Import prospects",
                        description: "Import prospects from the supplied list. optionally assign a sequence step, verify email addresses, and choose how to handle existing prospects."
                    },
                    {
                        name: "Import Prospects Using Field Names",
                        value: "ProspectController_importProspectsV2",
                        action: "Import prospects using field names",
                        description: "Submit an asynchronous prospect import using existing field names. each request supports up to 100,000 prospects and 90 mb. the response includes a requestid for get /v1/prospects/import-status/{requestid}; import errors are returned in a CSV file."
                    },
                    {
                        name: "List",
                        value: "SequenceContactController_findProspects",
                        action: "List prospects",
                        description: "List prospects with search, pagination, sorting, and optional custom fields"
                    },
                    {
                        name: "List Notes For A",
                        value: "ProspectController_getProspectNotes",
                        action: "List notes for a prospect",
                        description: "Returns a paginated list of notes attached to the specified prospect, along with any draft note and pagination metadata. use the <code>skip</code> and <code>take</code> query parameters to page through results."
                    },
                    {
                        name: "List Prospect Tags",
                        value: "SequenceContactController_findTags",
                        action: "List prospect tags",
                        description: "List prospect tags, optionally filtered by tag name"
                    },
                    {
                        name: "Remove Tags From",
                        value: "SequenceContactController_unAssignTagsToProspects",
                        action: "Remove tags from prospects",
                        description: "Remove the specified tags from the identified prospects"
                    },
                    {
                        name: "Unsubscribe",
                        value: "SequenceContactController_unsubscribeProspects-postV1ProspectsUnsubscribe",
                        action: "Unsubscribe prospects",
                        description: "Unsubscribe the prospects identified in the request"
                    },
                    {
                        name: "Update Prospect Field Values",
                        value: "ProspectController_upsertAttribute",
                        action: "Update prospect field values",
                        description: "Update one field with fieldid and attributevalue, or update multiple fields with an attributes array of fieldid/value pairs. use field IDs from get /v1/fields. prospects."
                    },
                    {
                        name: "Update Prospect Statuses",
                        value: "SequenceContactController_updateProspects",
                        action: "Update prospect statuses",
                        description: "Update the status of the specified prospects. a pause delay applies when the requested status is paused."
                    }
                ]
            },
            {
                displayName: "Attribute ID",
                name: "attributeId",
                type: "string",
                default: "",
                required: true,
                description: "Enter prospect attribute ID",
                placeholder: "e.g. as5ax",
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_getProspectAttributeById"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_getProspectAttributeById"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Prospect ID",
                        name: "prospectId",
                        type: "string",
                        default: "",
                        description: "Enter prospect ID",
                        placeholder: "e.g. as5ax"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_getProspectAttributeById"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Request ID",
                name: "requestId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_getProspectImportStatus"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_getProspectImportStatus"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Prospect ID",
                name: "prospectId",
                type: "string",
                default: "",
                required: true,
                description: "Public prospect ID returned by prospect read/list APIs",
                placeholder: "e.g. 8PvBmrB7P7",
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_getProspectNotes"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_getProspectNotes"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Skip",
                        name: "skip",
                        type: "number",
                        default: 0,
                        description: "Number of notes to skip (offset)",
                        placeholder: "e.g. 0",
                        typeOptions: {
                            minValue: 0
                        }
                    },
                    {
                        displayName: "Take",
                        name: "take",
                        type: "number",
                        default: 20,
                        description: "Number of notes to return per page (max 100)",
                        placeholder: "e.g. 20",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 100
                        }
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_getProspectNotes"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Emails",
                name: "emails",
                type: "json",
                default: [],
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_getProspectsVerificationStatus"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_getProspectsVerificationStatus"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Conflict Action",
                name: "conflictAction",
                type: "options",
                default: "overwrite",
                required: true,
                description: "Action when a prospect already exists in the sequence: overwrite updates supplied fields but leaves blank cells unchanged; noupdate skips existing prospects; addmissingfields fills only empty fields; upsert updates supplied fields and adds missing ones, with blank cells clearing existing values",
                options: [
                    {
                        name: "AddMissingFields",
                        value: "addMissingFields"
                    },
                    {
                        name: "Create or Update",
                        value: "upsert"
                    },
                    {
                        name: "NoUpdate",
                        value: "noUpdate"
                    },
                    {
                        name: "Overwrite",
                        value: "overwrite"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_importProspects"
                        ]
                    }
                }
            },
            {
                displayName: "Prospect List",
                name: "prospectList",
                type: "json",
                default: {},
                required: true,
                description: "The array of prospect list. each prospect is represented by an array of fields.",
                placeholder: "e.g. [object Object]",
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_importProspects"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_importProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Step ID",
                        name: "stepId",
                        type: "number",
                        default: 0,
                        description: "Optional. set the step to which the prospect is to be added.",
                        placeholder: "e.g. 0VOLRwYe82"
                    },
                    {
                        displayName: "Verify Prospects",
                        name: "verifyProspects",
                        type: "boolean",
                        default: false,
                        description: "Whether optional. set to \"true\" if prospect's email address should be verified."
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_importProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Conflict Action",
                name: "conflictAction",
                type: "options",
                default: "overwrite",
                required: true,
                description: "Action when a prospect already exists in the sequence: overwrite updates supplied fields but leaves blank cells unchanged; noupdate skips existing prospects; addmissingfields fills only empty fields; upsert updates supplied fields and adds missing ones, with blank cells clearing existing values",
                options: [
                    {
                        name: "AddMissingFields",
                        value: "addMissingFields"
                    },
                    {
                        name: "Create or Update",
                        value: "upsert"
                    },
                    {
                        name: "NoUpdate",
                        value: "noUpdate"
                    },
                    {
                        name: "Overwrite",
                        value: "overwrite"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_importProspectsV2"
                        ]
                    }
                }
            },
            {
                displayName: "Prospect List",
                name: "prospectList",
                type: "json",
                default: [],
                required: true,
                description: "The array of prospect list. each prospect is represented by an array of fields.",
                placeholder: "e.g. [object Object],[object Object]",
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_importProspectsV2"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_importProspectsV2"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Step ID",
                        name: "stepId",
                        type: "string",
                        default: "",
                        description: "Optional. set the step to which the prospect is to be added.",
                        placeholder: "e.g. 0VOLRwYe82"
                    },
                    {
                        displayName: "Tags",
                        name: "tags",
                        type: "json",
                        default: [],
                        description: "Array of tags to be assigned to the imported prospects. if the tag does not exist, it will be created.",
                        placeholder: "e.g. tag1,tag2,tag3"
                    },
                    {
                        displayName: "Verify Prospects",
                        name: "verifyProspects",
                        type: "boolean",
                        default: false,
                        description: "Whether optional. set to \"true\" if prospect's email address should be verified."
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_importProspectsV2"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Prospect ID",
                name: "prospectId",
                type: "string",
                default: "",
                required: true,
                description: "Public prospect ID returned by prospect read/list APIs",
                placeholder: "e.g. 8PvBmrB7P7",
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_upsertAttribute"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_upsertAttribute"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Attribute Value",
                        name: "attributeValue",
                        type: "string",
                        default: "",
                        description: "The new value to set for the field specified by \"fieldid\". required when \"fieldid\" is provided.",
                        placeholder: "e.g. Smith"
                    },
                    {
                        displayName: "Attributes",
                        name: "attributes",
                        type: "json",
                        default: [],
                        description: "A list of field updates to apply in a single request. use this to update multiple fields at once. each item must specify a fieldid and the new value. when this is provided, \"fieldid\" and \"attributevalue\" are ignored.",
                        placeholder: "e.g. [object Object],[object Object]"
                    },
                    {
                        displayName: "Field ID",
                        name: "fieldId",
                        type: "string",
                        default: "",
                        description: "The ID of the field to update. use this for updating a single field. use the field IDs returned by the get /fields endpoint. required when \"attributes\" is not provided.",
                        placeholder: "e.g. 1qPB1GBBwD"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "ProspectController_upsertAttribute"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Tags",
                name: "tags",
                type: "json",
                default: [],
                required: true,
                description: "Names of the new tags to be created and attached to the given prospects",
                placeholder: "e.g. Tag1,Tag2",
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_assignTagsToProspects"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_assignTagsToProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Prospects",
                        name: "prospects",
                        type: "json",
                        default: [],
                        description: "IDs of the prospects",
                        placeholder: "e.g. 2dP27N0gZ4,bzZWZpl4wM,2dP27NrgZ3"
                    },
                    {
                        displayName: "Prospects Emails",
                        name: "prospectsEmails",
                        type: "json",
                        default: [],
                        description: "Email addresses associated with the prospects",
                        placeholder: "e.g. example2@example.com,example1@example.com"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_assignTagsToProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Page Size",
                name: "pageSize",
                type: "options",
                default: 100,
                required: true,
                description: "Number of items per page. values greater than 100 will be capped to 100. default is 100 if not provided.",
                placeholder: "e.g. 10",
                options: [
                    {
                        name: "10",
                        value: 10
                    },
                    {
                        name: "100",
                        value: 100
                    },
                    {
                        name: "20",
                        value: 20
                    },
                    {
                        name: "50",
                        value: 50
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_findProspects"
                        ]
                    }
                }
            },
            {
                displayName: "Sort",
                name: "sort",
                type: "options",
                default: "ASC",
                required: true,
                description: "Sort order for the results",
                placeholder: "e.g. ASC",
                options: [
                    {
                        name: "ASC",
                        value: "ASC"
                    },
                    {
                        name: "DESC",
                        value: "DESC"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_findProspects"
                        ]
                    }
                }
            },
            {
                displayName: "Sort By",
                name: "sortBy",
                type: "options",
                default: "createdAt",
                required: true,
                description: "Field to sort the results by. if not selected, default sorting is done by createdat.",
                placeholder: "e.g. createdAt",
                options: [
                    {
                        name: "CreatedAt",
                        value: "createdAt"
                    },
                    {
                        name: "Owner",
                        value: "owner"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_findProspects"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_findProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Include Custom Fields",
                        name: "includeCustomFields",
                        type: "string",
                        default: "",
                        description: "Whether to include custom fields in the response. use \"true\" or \"false\"."
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 1,
                        description: "Page number for pagination",
                        placeholder: "e.g. 1"
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Search by first name, last name and email",
                        placeholder: "e.g. John"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_findProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_findTags"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Search by tag name",
                        placeholder: "e.g. NewDeal"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_findTags"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Contact ID",
                name: "contactId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_getContactMinimalSequences"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_getContactMinimalSequences"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Body JSON",
                name: "bodyJson",
                type: "json",
                default: [],
                required: true,
                description: "Raw request body",
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_unAssignTagsToProspects"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_unAssignTagsToProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_unsubscribeProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Prospect IDs",
                name: "prospectIds",
                type: "json",
                default: [],
                required: true,
                description: "Array of prospect IDs to unsubscribe",
                placeholder: "e.g. 2dP27N0gZ4,bzZWZpl4wM",
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_unsubscribeProspects-postV1ProspectsUnsubscribe"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_unsubscribeProspects-postV1ProspectsUnsubscribe"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Prospect And Sequence IDs",
                name: "prospectAndSequenceIds",
                type: "json",
                default: [],
                required: true,
                description: "Array of prospect ID and step ID",
                placeholder: "e.g. [object Object]",
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_updateProspects"
                        ]
                    }
                }
            },
            {
                displayName: "Status",
                name: "status",
                type: "string",
                default: "",
                required: true,
                description: "Status type to update",
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_updateProspects"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_updateProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Pause Delay In Days",
                        name: "pauseDelayInDays",
                        type: "number",
                        default: 0,
                        description: "Optional property to define pause delay in days in case of pausing prospects, only used when status=paused"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospects"
                        ],
                        operation: [
                            "SequenceContactController_updateProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "schedules"
                        ]
                    }
                },
                default: "ScheduleController_createSchedule",
                options: [
                    {
                        name: "Create A",
                        value: "ScheduleController_createSchedule",
                        action: "Create schedule",
                        description: "Create a sending schedule using a name, iana time zone, and per-day timeslots. set isdefault=true to assign it automatically to new sequences without a scheduleid."
                    },
                    {
                        name: "List All",
                        value: "ScheduleController_getSchedules",
                        action: "List all schedules",
                        description: "List schedules available to the caller. use a schedule ID when creating or updating a sequence; the default schedule is assigned when no ID is supplied."
                    }
                ]
            },
            {
                displayName: "Name",
                name: "name",
                type: "string",
                default: "",
                required: true,
                description: "Human-readable label for the schedule",
                placeholder: "e.g. Weekdays 9-5",
                displayOptions: {
                    show: {
                        resource: [
                            "schedules"
                        ],
                        operation: [
                            "ScheduleController_createSchedule"
                        ]
                    }
                }
            },
            {
                displayName: "Time Slots",
                name: "timeSlots",
                type: "json",
                default: [],
                required: true,
                description: "Exactly 7 entries \u2014 one per day of the week (sunday through saturday). days with no sending windows should have an empty <code>slots</code> array.",
                placeholder: "e.g. [object Object],[object Object],[object Object],[object Object],[object Object],[object Object],[object Object]",
                displayOptions: {
                    show: {
                        resource: [
                            "schedules"
                        ],
                        operation: [
                            "ScheduleController_createSchedule"
                        ]
                    }
                }
            },
            {
                displayName: "Timezone",
                name: "timezone",
                type: "string",
                default: "",
                required: true,
                description: "Iana timezone identifier (e.g. <code>america/new_york</code>, <code>asia/kolkata</code>). must be a valid entry from the iana time zone database.",
                placeholder: "e.g. America/New_York",
                displayOptions: {
                    show: {
                        resource: [
                            "schedules"
                        ],
                        operation: [
                            "ScheduleController_createSchedule"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "schedules"
                        ],
                        operation: [
                            "ScheduleController_createSchedule"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Is Default",
                        name: "isDefault",
                        type: "boolean",
                        default: false,
                        description: "Whether true, mark this schedule as the account default. new sequences without an explicit scheduleid will use this schedule.",
                        placeholder: "e.g. false"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "schedules"
                        ],
                        operation: [
                            "ScheduleController_createSchedule"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "schedules"
                        ],
                        operation: [
                            "ScheduleController_getSchedules"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ]
                    }
                },
                default: "SequenceController_addContactsToSequence",
                options: [
                    {
                        name: "Add A New Variant To An Existing Step",
                        value: "SequenceController_createVariant",
                        action: "Add new variant to an existing step sequences",
                        description: "Append a variant to an existing step. email steps support up to 26 variants; other channels support one. the payload must match the step channel: email uses subject/content, linkedin connection requests use connectionnote, messages use message, and other supported channels use their documented payload. sequences."
                    },
                    {
                        name: "Add Accounts To A",
                        value: "SequenceController_addEmailAccountToSequence",
                        action: "Add accounts to a sequence",
                        description: "Attach the specified email accounts to the sequence"
                    },
                    {
                        name: "Add Prospects To A Sequence Step",
                        value: "SequenceController_addContactsToSequence",
                        action: "Add prospects to a sequence step",
                        description: "Add the specified contacts to a step in the sequence"
                    },
                    {
                        name: "Create A New",
                        value: "SequenceController_createSequence",
                        action: "Create new sequence",
                        description: "Create a sequence with a title and optional sending accounts and schedule. all supplied account and schedule IDs are validated before creation; if validation or account limits fail, no sequence is created. if omitted, the default sending account is attached."
                    },
                    {
                        name: "Create A New Step For A",
                        value: "SequenceController_createStep",
                        action: "Create new step for a sequence",
                        description: "Add a step to the sequence. provide a channel, absolutedays (1-999), and at least one channel-specific variant; email steps allow up to 26 variants, while each day can contain at most one email step. task-generating steps can include priority and assignee."
                    },
                    {
                        name: "Get Sequence Settings (Optionally Filter By Code)",
                        value: "SequenceController_getSequenceSettings",
                        action: "Get sequence settings optionally filter by code",
                        description: "Return the sequence settings, assigned schedule, priority distribution, and client. pass code (1-13) to retrieve one setting."
                    },
                    {
                        name: "Get Sequence Step Variants",
                        value: "SequenceController_getSequenceStepVariants",
                        action: "Get sequence step variants",
                        description: "Return the variants configured for the specified sequence step"
                    },
                    {
                        name: "Import Prospects Into A Sequence Using Field Names",
                        value: "SequenceController_importProspectsV2",
                        action: "Import prospects into a sequence using field names",
                        description: "Submit an asynchronous prospect import into a sequence using existing field names. each request supports up to 100,000 prospects and 90 mb. use the returned requestid with get /v1/prospects/import-status/{requestid}; errors are provided as a CSV link."
                    },
                    {
                        name: "List All Steps And Variants For A",
                        value: "SequenceController_getSequenceSteps",
                        action: "List all steps and variants for a sequence",
                        description: "Return every step in the sequence, in execution order, each with its variants and their payloads. useful for exporting a sequence or for inspecting what will be sent."
                    },
                    {
                        name: "List Sequence Sending Accounts",
                        value: "SequenceController_sequenceEmailAccountList",
                        action: "List sequence sending accounts",
                        description: "List sending email accounts attached to the specified sequence"
                    },
                    {
                        name: "List Sequences And Steps",
                        value: "SequenceController_getSequencesWithSteps",
                        action: "List sequences and steps",
                        description: "List sequences with their steps. filter by sequence title and paginate or sort the results."
                    },
                    {
                        name: "Pause Or Resume",
                        value: "SequenceController_updateSequenceStatus",
                        action: "Pause or resume sequences",
                        description: "Pause or resume the selected sequences"
                    },
                    {
                        name: "Remove Accounts From A",
                        value: "SequenceController_removeEmailAccountFromSequence",
                        action: "Remove accounts from a sequence",
                        description: "Remove the specified email accounts from the sequence"
                    },
                    {
                        name: "Send A Sequence Test Email",
                        value: "SequenceController_sendTestEmail",
                        action: "Send sequence test email",
                        description: "Send a test email from an account attached to the sequence using the supplied recipients, subject, and HTML content"
                    },
                    {
                        name: "Start Prospect Email Verification",
                        value: "SequenceController_verifyProspects",
                        action: "Start prospect email verification sequences",
                        description: "Start email verification for prospects in the specified sequence"
                    },
                    {
                        name: "Update A Step Variant For A",
                        value: "SequenceController_updateVariant",
                        action: "Update step variant for a sequence",
                        description: "Update the <code>payload</code>, <code>status</code>, <code>tasknote</code>, <code>priority</code>, <code>assigneeid</code>, or <code>attachmentids</code> of an existing variant. the <code>payload</code> shape must match the step's channel (see OpenAPI.md). sequences."
                    },
                    {
                        name: "Update Priority Distribution For A",
                        value: "SequenceController_updatePriorityDistribution",
                        action: "Update priority distribution for a sequence",
                        description: "Set the sequence email priority preset: 1 prioritizes follow-ups, 2 prioritizes new prospects, 3 balances steps, and 4 sends up to 80% from the first step. provide prioritydistributionid."
                    },
                    {
                        name: "Update Prospect Outcomes",
                        value: "SequenceController_updateProspectOutcome",
                        action: "Update prospect outcomes sequences",
                        description: "Set an outcome for the specified prospect emails, optionally limiting the update to a sequence and setting a deal value"
                    },
                    {
                        name: "Update Sequence Settings And/Or Schedule",
                        value: "SequenceController_updateSequenceSettings",
                        action: "Update sequence settings and or schedule",
                        description: "Update only the setting codes supplied; other settings stay unchanged. code 1 must contain {{link}} unless blank to disable it, and can use an accepted preset or custom HTML. codes 1 and 2 are mutually exclusive; setting one clears the other. sequences."
                    },
                    {
                        name: "Update The Schedule Assigned To A",
                        value: "SequenceController_updateSequenceSchedule",
                        action: "Update schedule assigned to a sequence",
                        description: "Assign a schedule to the sequence by passing its hashed scheduleid. the schedule controls sending days and time windows; use get /v1/schedules to list available schedules."
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_addContactsToSequence"
                        ]
                    }
                }
            },
            {
                displayName: "Contact IDs",
                name: "contactIds",
                type: "json",
                default: [],
                required: true,
                description: "Contact IDs to add",
                placeholder: "e.g. 2dP27N0gZ4,VMw56r9jPb",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_addContactsToSequence"
                        ]
                    }
                }
            },
            {
                displayName: "Step ID",
                name: "stepId",
                type: "string",
                default: "",
                required: true,
                description: "Sequence step ID",
                placeholder: "e.g. bwOLEx4l8G",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_addContactsToSequence"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_addContactsToSequence"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the sequence to which the email accounts will be attached",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_addEmailAccountToSequence"
                        ]
                    }
                }
            },
            {
                displayName: "Email Account IDs",
                name: "emailAccountIds",
                type: "json",
                default: [],
                required: true,
                description: "Array of email account IDs to be added",
                placeholder: "e.g. 1Gz3xlNwr9,ajzR8xpPAq,vXwAZr6P8q",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_addEmailAccountToSequence"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_addEmailAccountToSequence"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Title",
                name: "title",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createSequence"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createSequence"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Email Account IDs",
                        name: "emailAccountIds",
                        type: "json",
                        default: [],
                        description: "Array of hashed email account IDs to attach as senders of the sequence. if any ID is invalid, inactive, or exceeds the account limit, the entire request is rejected and no sequence is created.",
                        placeholder: "e.g. 1Gz3xlNwr9,ajzR8xpPAq"
                    },
                    {
                        displayName: "Schedule ID",
                        name: "scheduleId",
                        type: "string",
                        default: "",
                        description: "Hashed ID of the sending schedule to assign to the sequence"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createSequence"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the sequence to which the step will be added",
                placeholder: "e.g. JA5YdAr9wy",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createStep"
                        ]
                    }
                }
            },
            {
                displayName: "Absolute Days",
                name: "absoluteDays",
                type: "number",
                default: 0,
                required: true,
                description: "The absolute number of days for the sequence step",
                placeholder: "e.g. 3",
                typeOptions: {
                    minValue: 1,
                    maxValue: 999
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createStep"
                        ]
                    }
                }
            },
            {
                displayName: "Type",
                name: "type",
                type: "options",
                default: "Email",
                required: true,
                description: "The task 'type' of step that will be created",
                placeholder: "e.g. 1",
                options: [
                    {
                        name: "CallDemo",
                        value: "CallDemo"
                    },
                    {
                        name: "CallFollowUp",
                        value: "CallFollowUp"
                    },
                    {
                        name: "CallIntroduction",
                        value: "CallIntroduction"
                    },
                    {
                        name: "CallOther",
                        value: "CallOther"
                    },
                    {
                        name: "CallReminder",
                        value: "CallReminder"
                    },
                    {
                        name: "Custom",
                        value: "Custom"
                    },
                    {
                        name: "Email",
                        value: "Email"
                    },
                    {
                        name: "LinkedInConnectionRequest",
                        value: "LinkedInConnectionRequest"
                    },
                    {
                        name: "LinkedInInMail",
                        value: "LinkedInInMail"
                    },
                    {
                        name: "LinkedInMessage",
                        value: "LinkedInMessage"
                    },
                    {
                        name: "LinkedInPostInteration",
                        value: "LinkedInPostInteration"
                    },
                    {
                        name: "LinkedInViewProfile",
                        value: "LinkedInViewProfile"
                    },
                    {
                        name: "WhatsappMessage",
                        value: "WhatsappMessage"
                    },
                    {
                        name: "WhatsappVoiceCall",
                        value: "WhatsappVoiceCall"
                    },
                    {
                        name: "WhatsappVoiceMessage",
                        value: "WhatsappVoiceMessage"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createStep"
                        ]
                    }
                }
            },
            {
                displayName: "Variants",
                name: "variants",
                type: "json",
                default: [],
                required: true,
                description: "Email steps support up to 26 variants for a/b testing; other channels use one. payload fields depend on the channel. email uses subject, content, and optional preheader; task-based channels may use tasknote, but email steps do not.",
                placeholder: "e.g. [object Object]",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createStep"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createStep"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Assignee ID",
                        name: "assigneeId",
                        type: "string",
                        default: "",
                        description: "User ID of the assignee who will be assigned the tasks generated by this step, default = user ID of sequence owner",
                        placeholder: "e.g. z6R8Mw4vBn"
                    },
                    {
                        displayName: "Priority",
                        name: "priority",
                        type: "options",
                        default: "Urgent",
                        description: "Priority of the tasks that will be generated by this step, default is normal",
                        placeholder: "e.g. 3",
                        options: [
                            {
                                name: "High",
                                value: "High"
                            },
                            {
                                name: "Low",
                                value: "Low"
                            },
                            {
                                name: "Normal",
                                value: "Normal"
                            },
                            {
                                name: "Urgent",
                                value: "Urgent"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createStep"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the sequence",
                placeholder: "e.g. k4PeeMxkPn",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createVariant"
                        ]
                    }
                }
            },
            {
                displayName: "Step ID",
                name: "stepId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the step",
                placeholder: "e.g. lXwAoMl4a8",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createVariant"
                        ]
                    }
                }
            },
            {
                displayName: "Payload",
                name: "payload",
                type: "json",
                default: {},
                required: true,
                description: "Variant payload by channel: email uses subject, content, and optional preheader; linkedin connection requests use connectionnote; linkedin messages use message; inmail uses subject and message; view-profile, post-interaction, task, call, and whatsapp steps use an empty object",
                placeholder: "e.g. [object Object]",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createVariant"
                        ]
                    }
                }
            },
            {
                displayName: "Type",
                name: "type",
                type: "options",
                default: "Email",
                required: true,
                description: "Channel code: 1 email; 2 linkedin connection request; 3 linkedin message; 4 linkedin inmail; 5 linkedin profile view; 6 linkedin post interaction; 9 task; 11 call introduction; 12 call demo; 13 call follow-up; 14 call reminder; 15 call other; 16 whatsapp message; 17 whatsapp voice message; 18 whatsapp voice call",
                placeholder: "e.g. 1",
                options: [
                    {
                        name: "CallDemo",
                        value: "CallDemo"
                    },
                    {
                        name: "CallFollowUp",
                        value: "CallFollowUp"
                    },
                    {
                        name: "CallIntroduction",
                        value: "CallIntroduction"
                    },
                    {
                        name: "CallOther",
                        value: "CallOther"
                    },
                    {
                        name: "CallReminder",
                        value: "CallReminder"
                    },
                    {
                        name: "Custom",
                        value: "Custom"
                    },
                    {
                        name: "Email",
                        value: "Email"
                    },
                    {
                        name: "LinkedInConnectionRequest",
                        value: "LinkedInConnectionRequest"
                    },
                    {
                        name: "LinkedInInMail",
                        value: "LinkedInInMail"
                    },
                    {
                        name: "LinkedInMessage",
                        value: "LinkedInMessage"
                    },
                    {
                        name: "LinkedInPostInteration",
                        value: "LinkedInPostInteration"
                    },
                    {
                        name: "LinkedInViewProfile",
                        value: "LinkedInViewProfile"
                    },
                    {
                        name: "WhatsappMessage",
                        value: "WhatsappMessage"
                    },
                    {
                        name: "WhatsappVoiceCall",
                        value: "WhatsappVoiceCall"
                    },
                    {
                        name: "WhatsappVoiceMessage",
                        value: "WhatsappVoiceMessage"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createVariant"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createVariant"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Absolute Days",
                        name: "absoluteDays",
                        type: "number",
                        default: 0,
                        description: "Override day number for this variant. defaults to the step's absolutedays.",
                        placeholder: "e.g. 3",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Assignee ID",
                        name: "assigneeId",
                        type: "string",
                        default: "",
                        description: "Hashed user ID to assign generated tasks to (task-based channels). defaults to the sequence owner.",
                        placeholder: "e.g. z6R8Mw4vBn"
                    },
                    {
                        displayName: "Attachment IDs",
                        name: "attachmentIds",
                        type: "json",
                        default: [],
                        description: "Hashed IDs of attachments to include (email variants only). upload via post /v1/attachments to obtain an ID.",
                        placeholder: "e.g. lN5xKp2vJq"
                    },
                    {
                        displayName: "Priority",
                        name: "priority",
                        type: "options",
                        default: "Urgent",
                        description: "Task priority (1=urgent, 2=high, 3=normal, 4=low). used when the channel generates a task.",
                        placeholder: "e.g. 3",
                        options: [
                            {
                                name: "High",
                                value: "High"
                            },
                            {
                                name: "Low",
                                value: "Low"
                            },
                            {
                                name: "Normal",
                                value: "Normal"
                            },
                            {
                                name: "Urgent",
                                value: "Urgent"
                            }
                        ]
                    },
                    {
                        displayName: "Task Note",
                        name: "taskNote",
                        type: "string",
                        default: "",
                        description: "Task note for the generated task. not allowed on email variants (type=1) \u2014 edge rejects it because email is automated, not task-based. use for linkedin / call / task / whatsapp channels.",
                        placeholder: "e.g. Follow up within 24 hours if no reply"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_createVariant"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the sequence",
                placeholder: "e.g. JA5YdAr9wy",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_getSequenceSettings"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_getSequenceSettings"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Code",
                        name: "code",
                        type: "number",
                        default: 0,
                        description: "Filter to a single setting code (1\u201313). omit to return all settings."
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_getSequenceSettings"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the sequence",
                placeholder: "e.g. k4PeeMxkPn",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_getSequenceStepVariants"
                        ]
                    }
                }
            },
            {
                displayName: "Step ID",
                name: "stepId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the step",
                placeholder: "e.g. lXwAoMl4a8",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_getSequenceStepVariants"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_getSequenceStepVariants"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the sequence",
                placeholder: "e.g. JA5YdAr9wy",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_getSequenceSteps"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_getSequenceSteps"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_getSequencesWithSteps"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Client IDs",
                        name: "clientIds",
                        type: "string",
                        default: ""
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 1,
                        description: "The page number for pagination",
                        placeholder: "e.g. 1"
                    },
                    {
                        displayName: "Page Size",
                        name: "pageSize",
                        type: "number",
                        default: 100,
                        description: "The number of items per page in the result. maximum pagesize is 1000.",
                        placeholder: "e.g. 5"
                    },
                    {
                        displayName: "Sequence Name",
                        name: "sequenceName",
                        type: "string",
                        default: "",
                        description: "Search by sequence title",
                        placeholder: "e.g. sequence"
                    },
                    {
                        displayName: "Sort",
                        name: "sort",
                        type: "options",
                        default: "ASC",
                        description: "The sorting order for the result",
                        placeholder: "e.g. ASC",
                        options: [
                            {
                                name: "ASC",
                                value: "ASC"
                            },
                            {
                                name: "DESC",
                                value: "DESC"
                            }
                        ]
                    },
                    {
                        displayName: "Sort By",
                        name: "sortBy",
                        type: "options",
                        default: "sequence.createdAt",
                        description: "The field by which the result should be sorted",
                        placeholder: "e.g. sequence.createdAt",
                        options: [
                            {
                                name: "Sequence.CreatedAt",
                                value: "sequence.createdAt"
                            },
                            {
                                name: "Sequence.Title",
                                value: "sequence.title"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_getSequencesWithSteps"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Conflict Action",
                name: "conflictAction",
                type: "options",
                default: "overwrite",
                required: true,
                description: "Action when a prospect already exists in the sequence: overwrite updates supplied fields but leaves blank cells unchanged; noupdate skips existing prospects; addmissingfields fills only empty fields; upsert updates supplied fields and adds missing ones, with blank cells clearing existing values",
                options: [
                    {
                        name: "AddMissingFields",
                        value: "addMissingFields"
                    },
                    {
                        name: "Create or Update",
                        value: "upsert"
                    },
                    {
                        name: "NoUpdate",
                        value: "noUpdate"
                    },
                    {
                        name: "Overwrite",
                        value: "overwrite"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_importProspectsV2"
                        ]
                    }
                }
            },
            {
                displayName: "Prospect List",
                name: "prospectList",
                type: "json",
                default: [],
                required: true,
                description: "The array of prospect list. each prospect is represented by an array of fields.",
                placeholder: "e.g. [object Object],[object Object]",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_importProspectsV2"
                        ]
                    }
                }
            },
            {
                displayName: "Step ID",
                name: "stepId",
                type: "number",
                default: 0,
                required: true,
                description: "Set the step to which the prospect is to be added",
                placeholder: "e.g. 0VOLRwYe82",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_importProspectsV2"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_importProspectsV2"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Tags",
                        name: "tags",
                        type: "json",
                        default: [],
                        description: "Array of tags to be assigned to the imported prospects. if the tag does not exist, it will be created.",
                        placeholder: "e.g. tag1,tag2,tag3"
                    },
                    {
                        displayName: "Verify Prospects",
                        name: "verifyProspects",
                        type: "boolean",
                        default: false,
                        description: "Whether optional. set to \"true\" if prospect's email address should be verified."
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_importProspectsV2"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the sequence from which the email accounts will be removed",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_removeEmailAccountFromSequence"
                        ]
                    }
                }
            },
            {
                displayName: "Email Account IDs",
                name: "emailAccountIds",
                type: "json",
                default: [],
                required: true,
                description: "Array of email account IDs to be removed",
                placeholder: "e.g. 1Gz3xlNwr9,ajzR8xpPAq,vXwAZr6P8q",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_removeEmailAccountFromSequence"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_removeEmailAccountFromSequence"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "Public sequence ID",
                placeholder: "e.g. JA5YdAr9wy",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_sendTestEmail"
                        ]
                    }
                }
            },
            {
                displayName: "Content",
                name: "content",
                type: "string",
                default: "",
                required: true,
                description: "HTML body of the test email",
                placeholder: "e.g. <p>Hello from OpenAPI validation.</p>",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_sendTestEmail"
                        ]
                    }
                }
            },
            {
                displayName: "From Email Account ID",
                name: "fromEmailAccountId",
                type: "string",
                default: "",
                required: true,
                description: "Public sending email account ID. the sender must be connected to the sequence.",
                placeholder: "e.g. Y8aL4J6jaN",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_sendTestEmail"
                        ]
                    }
                }
            },
            {
                displayName: "Subject",
                name: "subject",
                type: "string",
                default: "",
                required: true,
                description: "Subject line of the test email",
                placeholder: "e.g. OpenAPI test email",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_sendTestEmail"
                        ]
                    }
                }
            },
            {
                displayName: "To",
                name: "to",
                type: "json",
                default: [],
                required: true,
                description: "One or more recipient email addresses for the test email",
                placeholder: "e.g. qa.recipient@example.org",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_sendTestEmail"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_sendTestEmail"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Preheader",
                        name: "preheader",
                        type: "string",
                        default: "",
                        description: "Preview text shown in the recipient inbox",
                        placeholder: "e.g. OpenAPI preheader"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_sendTestEmail"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the sequence to which the email accounts will be attached",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_sequenceEmailAccountList"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_sequenceEmailAccountList"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the sequence",
                placeholder: "e.g. JA5YdAr9wy",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updatePriorityDistribution"
                        ]
                    }
                }
            },
            {
                displayName: "Priority Distribution ID",
                name: "priorityDistributionId",
                type: "options",
                default: "PrioritiseFollowUps",
                required: true,
                description: "Priority distribution preset ID. `1` = prioritise follow-ups (more emails from follow-up steps). `2` = prioritise new prospects (more emails from the first step). `3` = balanced sending (equal across steps). `4` = aggressively prioritise new prospects (~80% from the first step).",
                placeholder: "e.g. 3",
                options: [
                    {
                        name: "AggressivelyPrioritiseNewProspects",
                        value: "AggressivelyPrioritiseNewProspects"
                    },
                    {
                        name: "BalancedSending",
                        value: "BalancedSending"
                    },
                    {
                        name: "PrioritiseFollowUps",
                        value: "PrioritiseFollowUps"
                    },
                    {
                        name: "PrioritiseNewProspects",
                        value: "PrioritiseNewProspects"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updatePriorityDistribution"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updatePriorityDistribution"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Outcome Name",
                name: "outcomeName",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateProspectOutcome"
                        ]
                    }
                }
            },
            {
                displayName: "Prospect Emails",
                name: "prospectEmails",
                type: "json",
                default: [],
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateProspectOutcome"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateProspectOutcome"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Deal Value",
                        name: "dealValue",
                        type: "number",
                        default: 0
                    },
                    {
                        displayName: "Sequence ID",
                        name: "sequenceId",
                        type: "string",
                        default: ""
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateProspectOutcome"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the sequence",
                placeholder: "e.g. JA5YdAr9wy",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateSequenceSchedule"
                        ]
                    }
                }
            },
            {
                displayName: "Schedule ID",
                name: "scheduleId",
                type: "string",
                default: "",
                required: true,
                description: "Hashed ID of the schedule to assign to the sequence",
                placeholder: "e.g. aB3xK9",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateSequenceSchedule"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateSequenceSchedule"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the sequence",
                placeholder: "e.g. JA5YdAr9wy",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateSequenceSettings"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateSequenceSettings"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Schedule ID",
                        name: "scheduleId",
                        type: "string",
                        default: "",
                        description: "Hashed ID of the schedule to assign to this sequence"
                    },
                    {
                        displayName: "Settings",
                        name: "settings",
                        type: "json",
                        default: [],
                        description: "Array of settings to update. all codes are forwarded."
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateSequenceSettings"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence IDs",
                name: "sequenceIds",
                type: "json",
                default: [],
                required: true,
                description: "IDs of the sequences",
                placeholder: "e.g. 2dP27N0gZ4,2dP27NugZ3,2dP27NrgZ3",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateSequenceStatus"
                        ]
                    }
                }
            },
            {
                displayName: "Status",
                name: "status",
                type: "options",
                default: "resume",
                required: true,
                placeholder: "e.g. resume",
                options: [
                    {
                        name: "Pause",
                        value: "pause"
                    },
                    {
                        name: "Resume",
                        value: "resume"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateSequenceStatus"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateSequenceStatus"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the sequence",
                placeholder: "e.g. k4PeeMxkPn",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateVariant"
                        ]
                    }
                }
            },
            {
                displayName: "Step ID",
                name: "stepId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the step",
                placeholder: "e.g. lXwAoMl4a8",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateVariant"
                        ]
                    }
                }
            },
            {
                displayName: "Variant ID",
                name: "variantId",
                type: "string",
                default: "",
                required: true,
                description: "The ID of the variant",
                placeholder: "e.g. 1qPBgAB4aD",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateVariant"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateVariant"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Absolute Days",
                        name: "absoluteDays",
                        type: "number",
                        default: 0,
                        description: "Override day number for this variant (1\u2013999). each day can hold at most one email step.",
                        placeholder: "e.g. 3",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Assignee ID",
                        name: "assigneeId",
                        type: "string",
                        default: "",
                        description: "Hashed user ID to assign generated tasks to (task-based channels). defaults to the sequence owner.",
                        placeholder: "e.g. z6R8Mw4vBn"
                    },
                    {
                        displayName: "Attachment IDs",
                        name: "attachmentIds",
                        type: "json",
                        default: [],
                        description: "Full replacement of the attachment list for this variant (email variants only). pass `[]` to remove all attachments. upload attachments via post /v1/attachments to obtain an ID.",
                        placeholder: "e.g. lN5xKp2vJq,aQ8dVzYw1R"
                    },
                    {
                        displayName: "Payload",
                        name: "payload",
                        type: "json",
                        default: {},
                        description: "Payload for the variant. shape must match the step's channel \u2014 see OpenAPI.md (email: `{subject, content, preheader?}`, linkedinconnectionrequest: `{connectionnote}`, linkedinmessage: `{message}`, linkedininmail: `{subject, message}`, viewprofile/postinteraction/task/call/whatsapp: `{}`).",
                        placeholder: "e.g. [object Object]"
                    },
                    {
                        displayName: "Priority",
                        name: "priority",
                        type: "options",
                        default: "Urgent",
                        description: "Task priority (1=urgent, 2=high, 3=normal, 4=low). used when the channel generates a task.",
                        placeholder: "e.g. 3",
                        options: [
                            {
                                name: "High",
                                value: "High"
                            },
                            {
                                name: "Low",
                                value: "Low"
                            },
                            {
                                name: "Normal",
                                value: "Normal"
                            },
                            {
                                name: "Urgent",
                                value: "Urgent"
                            }
                        ]
                    },
                    {
                        displayName: "Status",
                        name: "status",
                        type: "options",
                        default: 0,
                        description: "Variant status: 0 = inactive (paused), 1 = active",
                        placeholder: "e.g. 1",
                        options: [
                            {
                                name: "0",
                                value: 0
                            },
                            {
                                name: "1",
                                value: 1
                            }
                        ]
                    },
                    {
                        displayName: "Task Note",
                        name: "taskNote",
                        type: "string",
                        default: "",
                        description: "Task note for the variant. not allowed on email variants (type=1) \u2014 edge rejects it because email is automated, not task-based. use for linkedin / call / task / whatsapp channels.",
                        placeholder: "e.g. Follow up within 24 hours if no reply"
                    },
                    {
                        displayName: "Type",
                        name: "type",
                        type: "options",
                        default: "Email",
                        description: "Channel type of the variant. rarely changed on update \u2014 must still match the parent step.",
                        placeholder: "e.g. 1",
                        options: [
                            {
                                name: "CallDemo",
                                value: "CallDemo"
                            },
                            {
                                name: "CallFollowUp",
                                value: "CallFollowUp"
                            },
                            {
                                name: "CallIntroduction",
                                value: "CallIntroduction"
                            },
                            {
                                name: "CallOther",
                                value: "CallOther"
                            },
                            {
                                name: "CallReminder",
                                value: "CallReminder"
                            },
                            {
                                name: "Custom",
                                value: "Custom"
                            },
                            {
                                name: "Email",
                                value: "Email"
                            },
                            {
                                name: "LinkedInConnectionRequest",
                                value: "LinkedInConnectionRequest"
                            },
                            {
                                name: "LinkedInInMail",
                                value: "LinkedInInMail"
                            },
                            {
                                name: "LinkedInMessage",
                                value: "LinkedInMessage"
                            },
                            {
                                name: "LinkedInPostInteration",
                                value: "LinkedInPostInteration"
                            },
                            {
                                name: "LinkedInViewProfile",
                                value: "LinkedInViewProfile"
                            },
                            {
                                name: "WhatsappMessage",
                                value: "WhatsappMessage"
                            },
                            {
                                name: "WhatsappVoiceCall",
                                value: "WhatsappVoiceCall"
                            },
                            {
                                name: "WhatsappVoiceMessage",
                                value: "WhatsappVoiceMessage"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_updateVariant"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "Public sequence ID",
                placeholder: "e.g. JA5YdAr9wy",
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_verifyProspects"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequences"
                        ],
                        operation: [
                            "SequenceController_verifyProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ]
                    }
                },
                default: "SubsequenceController_createSubsequence",
                options: [
                    {
                        name: "Create A New Subsequence Under A Parent Sequence",
                        value: "SubsequenceController_createSubsequence",
                        action: "Create new subsequence under a parent sequence",
                        description: "Create a subsequence with a 1-200 character title and at least one trigger condition; conditions use and logic. a default schedule is used if scheduleid is omitted. firststeprelativedays must be at least 1 when set. add steps after creation using the returned ID."
                    },
                    {
                        name: "Get Schedule, Entry Delay, & Trigger Conditions",
                        value: "SubsequenceController_getSubsequenceSettings",
                        action: "Get schedule entry delay trigger conditions subsequences",
                        description: "Retrieve the current <code>scheduleid</code>, <code>firststeprelativedays</code>, and <code>conditions[]</code> configured on a subsequence. use after create subsequence or update subsequence to read the live state."
                    },
                    {
                        name: "List All Subsequences Under A Parent Sequence",
                        value: "SubsequenceController_listSubsequences",
                        action: "List all subsequences under a parent sequence",
                        description: "List subsequences with progress and engagement metrics. filter by progress (1 active, 2 paused, 3 finished), title search, or owner IDs."
                    },
                    {
                        name: "Update The Schedule, Entry Delay, Or Conditions",
                        value: "SubsequenceController_updateSubsequence",
                        action: "Update schedule entry delay or conditions subsequences",
                        description: "Update the schedule, entry delay, or conditions. omitted fields stay unchanged; conditions replace the full set, and an empty conditions array does not clear them. read the updated state with get /v1/sequences/subsequence/{subsequenceid}/settings."
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "Hashid-encoded ID of the parent sequence",
                placeholder: "e.g. JA5YdAr9wy",
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_createSubsequence"
                        ]
                    }
                }
            },
            {
                displayName: "Conditions",
                name: "conditions",
                type: "json",
                default: [],
                required: true,
                description: "Trigger conditions (min 1). all conditions use implicit and logic. each item is validated against the condition matrix \u2014 invalid name/operation/value combinations are rejected with a 400.",
                placeholder: "e.g. [object Object],[object Object],[object Object]",
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_createSubsequence"
                        ]
                    }
                }
            },
            {
                displayName: "Title",
                name: "title",
                type: "string",
                default: "",
                required: true,
                description: "Subsequence title. 1\u2013200 characters.",
                placeholder: "e.g. Re-engagement Subsequence",
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_createSubsequence"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_createSubsequence"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "First Step Relative Days",
                        name: "firstStepRelativeDays",
                        type: "number",
                        default: 0,
                        description: "Days after the trigger fires before the first step runs (entry delay). minimum 1. when omitted, no delay is applied.",
                        placeholder: "e.g. 2",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Schedule ID",
                        name: "scheduleId",
                        type: "number",
                        default: 0,
                        description: "Sending schedule ID (integer). use <code>get /v1/schedules</code> to list available IDs. when omitted, the account default schedule is assigned.",
                        placeholder: "e.g. 58964"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_createSubsequence"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Subsequence ID",
                name: "subsequenceId",
                type: "string",
                default: "",
                required: true,
                description: "Hashid-encoded ID of the subsequence",
                placeholder: "e.g. JA5YdAr9wy",
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_getSubsequenceSettings"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_getSubsequenceSettings"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "Hashid-encoded ID of the parent sequence",
                placeholder: "e.g. JA5YdAr9wy",
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_listSubsequences"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_listSubsequences"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Owners",
                        name: "owners",
                        type: "json",
                        default: [],
                        description: "Filter by owner user IDs (hashid-encoded strings). accepts repeating values (<code>?owners=abc&owners=def</code>). omit to return all subsequences scoped to the authenticated account.",
                        placeholder: "e.g. JA5YdAr9wy"
                    },
                    {
                        displayName: "Progress",
                        name: "progress",
                        type: "json",
                        default: [],
                        description: "Filter by subsequence progress. accepts repeating values (<code>?progress=1&progress=2</code>). <code>1</code>=active, <code>2</code>=paused, <code>3</code>=finished.",
                        placeholder: "e.g. 1,2"
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Filter by subsequence title (partial match)",
                        placeholder: "e.g. re-engagement"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_listSubsequences"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Sequence ID",
                name: "sequenceId",
                type: "string",
                default: "",
                required: true,
                description: "Hashid-encoded ID of the parent sequence",
                placeholder: "e.g. JA5YdAr9wy",
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_updateSubsequence"
                        ]
                    }
                }
            },
            {
                displayName: "Subsequence ID",
                name: "subsequenceId",
                type: "string",
                default: "",
                required: true,
                description: "Hashid-encoded ID of the subsequence to update",
                placeholder: "e.g. k4PeeMxkPn",
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_updateSubsequence"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_updateSubsequence"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Conditions",
                        name: "conditions",
                        type: "json",
                        default: [],
                        description: "Replace all current conditions with this new set (full-replacement, not append). must contain at least 1 item when supplied. passing <code>conditions: []</code> is silently ignored by the internal API \u2014 does not clear conditions.",
                        placeholder: "e.g. [object Object],[object Object]"
                    },
                    {
                        displayName: "First Step Relative Days",
                        name: "firstStepRelativeDays",
                        type: "number",
                        default: 0,
                        description: "Replace the entry delay (days after the trigger before the first step runs). minimum 1.",
                        placeholder: "e.g. 3",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Schedule ID",
                        name: "scheduleId",
                        type: "number",
                        default: 0,
                        description: "Replace the current sending schedule. use <code>get /v1/schedules</code> to list available IDs.",
                        placeholder: "e.g. 60030"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "subsequences"
                        ],
                        operation: [
                            "SubsequenceController_updateSubsequence"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ]
                    }
                },
                default: "TaskController_bulkSkipTask",
                options: [
                    {
                        name: "Complete A",
                        value: "TaskController_completeTask",
                        action: "Complete task",
                        description: "Mark the specified task complete, optionally including a call outcome"
                    },
                    {
                        name: "Create A",
                        value: "TaskController_createTask",
                        action: "Create task",
                        description: "Create a task for one prospect using prospectid, or create the same task for several prospects using prospectids. the response reports each prospect result separately, so one failure does not affect the others."
                    },
                    {
                        name: "Get Bulk Task Status",
                        value: "TaskController_getBulkTaskStatus",
                        action: "Get bulk task status",
                        description: "Check the progress of a bulk snooze or skip operation. the response includes error details and a CSV link when applicable. tasks."
                    },
                    {
                        name: "Get Task Counts",
                        value: "TaskController_getTaskCounts",
                        action: "Get task counts",
                        description: "Return task counts grouped by status"
                    },
                    {
                        name: "Get Task Details",
                        value: "TaskController_getTaskById",
                        action: "Get task details",
                        description: "Return details for the specified task"
                    },
                    {
                        name: "List",
                        value: "TaskController_getTasks",
                        action: "List tasks",
                        description: "List tasks with filters for status, sequence, assignee, prospect outcome, task type, and call outcome"
                    },
                    {
                        name: "List Task Assignees",
                        value: "TaskController_getAssigneeList",
                        action: "List task assignees",
                        description: "List users available to assign to tasks"
                    },
                    {
                        name: "Skip A",
                        value: "TaskController_skipTask",
                        action: "Skip task",
                        description: "Skip the specified task"
                    },
                    {
                        name: "Skip Multiple Tasks At The Same Time",
                        value: "TaskController_bulkSkipTask",
                        action: "Skip multiple tasks at the same time",
                        description: "Skip up to 100 tasks asynchronously. the response returns a bulkactionid; check get /v1/tasks/bulk-status/{bulkactionid} for progress and errors."
                    },
                    {
                        name: "Snooze A",
                        value: "TaskController_snoozeTask",
                        action: "Snooze task",
                        description: "Snooze the specified task until the supplied time"
                    },
                    {
                        name: "Snooze Multiple Tasks With Snooze Duration Or Time",
                        value: "TaskController_bulkSnoozeTask",
                        action: "Snooze multiple tasks with snooze duration or time",
                        description: "Snooze up to 10,000 tasks asynchronously. the response returns a bulkactionid; check get /v1/tasks/bulk-status/{bulkactionid} for progress and errors."
                    },
                    {
                        name: "Update A Task Note",
                        value: "TaskController_updateTaskNote",
                        action: "Update task note",
                        description: "Update the note on the specified task"
                    }
                ]
            },
            {
                displayName: "Task IDs",
                name: "taskIds",
                type: "json",
                default: [],
                required: true,
                description: "Array of task IDs to skip",
                placeholder: "e.g. lP0Zg2keMz,MP0Cg1keMa,BS0Zg5keMq",
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_bulkSkipTask"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_bulkSkipTask"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Snooze Until",
                name: "snoozeUntil",
                type: "dateTime",
                default: "",
                required: true,
                description: "Date and time until which the tasks should be snoozed",
                placeholder: "e.g. 2024-06-17T05:55:38.434Z",
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_bulkSnoozeTask"
                        ]
                    }
                }
            },
            {
                displayName: "Task IDs",
                name: "taskIds",
                type: "json",
                default: [],
                required: true,
                description: "Array of task IDs to snooze",
                placeholder: "e.g. lP0Zg2keMz,MP0Cg1keMa,BS0Zg5keMq",
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_bulkSnoozeTask"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_bulkSnoozeTask"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Task ID",
                name: "taskId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_completeTask"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_completeTask"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Call Outcome",
                        name: "callOutcome",
                        type: "options",
                        default: "Interested",
                        description: "Call outcome for call tasks (mandatory for call tasks)",
                        placeholder: "e.g. no-answer",
                        options: [
                            {
                                name: "Callback Later",
                                value: "Callback Later"
                            },
                            {
                                name: "Followup",
                                value: "Followup"
                            },
                            {
                                name: "Interested",
                                value: "Interested"
                            },
                            {
                                name: "Left Voicemail",
                                value: "Left Voicemail"
                            },
                            {
                                name: "No Answer",
                                value: "No Answer"
                            },
                            {
                                name: "Not In Service",
                                value: "Not In Service"
                            },
                            {
                                name: "Not Interested",
                                value: "Not Interested"
                            },
                            {
                                name: "Wrong Number",
                                value: "Wrong Number"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_completeTask"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Prospect IDs",
                name: "prospectIds",
                type: "json",
                default: [],
                required: true,
                description: "List of prospect IDs to create the task for. pass a single-element array for one prospect, or multiple IDs to create the same task for several prospects in one request.",
                placeholder: "e.g. 8PvBmrB7P7",
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_createTask"
                        ]
                    }
                }
            },
            {
                displayName: "Task Type",
                name: "taskType",
                type: "options",
                default: "2",
                required: true,
                description: "Task type code: 2 linkedin connection request; 3 linkedin message; 4 linkedin inmail; 5 linkedin profile view; 6 linkedin post interaction; 9 custom; 11 call introduction; 12 call demo; 13 call follow-up; 14 call reminder; 15 call other; 16 whatsapp message; 17 whatsapp voice message; 18 whatsapp voice call",
                placeholder: "e.g. 13",
                options: [
                    {
                        name: "11",
                        value: "11"
                    },
                    {
                        name: "12",
                        value: "12"
                    },
                    {
                        name: "13",
                        value: "13"
                    },
                    {
                        name: "14",
                        value: "14"
                    },
                    {
                        name: "15",
                        value: "15"
                    },
                    {
                        name: "16",
                        value: "16"
                    },
                    {
                        name: "17",
                        value: "17"
                    },
                    {
                        name: "18",
                        value: "18"
                    },
                    {
                        name: "2",
                        value: "2"
                    },
                    {
                        name: "3",
                        value: "3"
                    },
                    {
                        name: "4",
                        value: "4"
                    },
                    {
                        name: "5",
                        value: "5"
                    },
                    {
                        name: "6",
                        value: "6"
                    },
                    {
                        name: "9",
                        value: "9"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_createTask"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_createTask"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Due Date",
                        name: "dueDate",
                        type: "string",
                        default: "",
                        description: "The due date for the task in yyyy-mm-dd format (e.g. \"2026-04-22\"). defaults to today if not provided.",
                        placeholder: "e.g. 2026-04-22"
                    },
                    {
                        displayName: "Priority",
                        name: "priority",
                        type: "options",
                        default: "1",
                        description: "The priority level of the task. accepted values: \"1\" = urgent, \"2\" = high, \"3\" = medium, \"4\" = low. defaults to no priority if not provided.",
                        placeholder: "e.g. 3",
                        options: [
                            {
                                name: "1",
                                value: "1"
                            },
                            {
                                name: "2",
                                value: "2"
                            },
                            {
                                name: "3",
                                value: "3"
                            },
                            {
                                name: "4",
                                value: "4"
                            }
                        ]
                    },
                    {
                        displayName: "Task Note",
                        name: "taskNote",
                        type: "string",
                        default: "",
                        description: "An optional note or description to attach to the task",
                        placeholder: "e.g. Call to follow up on the proposal sent yesterday."
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_createTask"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_getAssigneeList"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Bulk Action ID",
                name: "bulkActionId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_getBulkTaskStatus"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_getBulkTaskStatus"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Task ID",
                name: "taskId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_getTaskById"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_getTaskById"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_getTaskCounts"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_getTasks"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Call Outcome",
                        name: "callOutcome",
                        type: "options",
                        default: "Interested",
                        description: "Call outcome for filtering tasks",
                        options: [
                            {
                                name: "Callback Later",
                                value: "Callback Later"
                            },
                            {
                                name: "Followup",
                                value: "Followup"
                            },
                            {
                                name: "Interested",
                                value: "Interested"
                            },
                            {
                                name: "Left Voicemail",
                                value: "Left Voicemail"
                            },
                            {
                                name: "No Answer",
                                value: "No Answer"
                            },
                            {
                                name: "Not In Service",
                                value: "Not In Service"
                            },
                            {
                                name: "Not Interested",
                                value: "Not Interested"
                            },
                            {
                                name: "Wrong Number",
                                value: "Wrong Number"
                            }
                        ]
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number for pagination",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page Size",
                        name: "pageSize",
                        type: "number",
                        default: 0,
                        description: "Number of records per page",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Prospect Outcome",
                        name: "prospectOutcome",
                        type: "string",
                        default: ""
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Search term to filter tasks"
                    },
                    {
                        displayName: "Sequence ID",
                        name: "sequenceId",
                        type: "string",
                        default: "",
                        description: "Sequence ID (hashed)"
                    },
                    {
                        displayName: "Sort By",
                        name: "sortBy",
                        type: "options",
                        default: "priority",
                        description: "Field to sort by",
                        options: [
                            {
                                name: "CompletedAt",
                                value: "completedAt"
                            },
                            {
                                name: "DueDate",
                                value: "dueDate"
                            },
                            {
                                name: "Priority",
                                value: "priority"
                            },
                            {
                                name: "SkippedAt",
                                value: "skippedAt"
                            }
                        ]
                    },
                    {
                        displayName: "Sort Order",
                        name: "sortOrder",
                        type: "options",
                        default: "ASC",
                        description: "Sort order for the results",
                        options: [
                            {
                                name: "ASC",
                                value: "ASC"
                            },
                            {
                                name: "DESC",
                                value: "DESC"
                            }
                        ]
                    },
                    {
                        displayName: "Status",
                        name: "status",
                        type: "options",
                        default: "upcoming",
                        description: "Status of the task",
                        options: [
                            {
                                name: "Completed",
                                value: "completed"
                            },
                            {
                                name: "DueToday",
                                value: "dueToday"
                            },
                            {
                                name: "Overdue",
                                value: "overdue"
                            },
                            {
                                name: "Skipped",
                                value: "skipped"
                            },
                            {
                                name: "Upcoming",
                                value: "upcoming"
                            }
                        ]
                    },
                    {
                        displayName: "Task Assignee",
                        name: "taskAssignee",
                        type: "string",
                        default: "",
                        description: "Task assignee account ID (hashed)"
                    },
                    {
                        displayName: "Task Type",
                        name: "taskType",
                        type: "options",
                        default: "linkedin",
                        description: "Open API task type",
                        options: [
                            {
                                name: "Call",
                                value: "call"
                            },
                            {
                                name: "Custom",
                                value: "custom"
                            },
                            {
                                name: "Linkedin",
                                value: "linkedin"
                            },
                            {
                                name: "Whatsapp",
                                value: "whatsapp"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_getTasks"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "status",
                    "callOutcome",
                    "page",
                    "pageSize",
                    "prospectOutcome",
                    "search",
                    "sequenceId",
                    "sortBy",
                    "sortOrder",
                    "taskAssignee"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_getTasks"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "CallOutcome",
                        value: "callOutcome"
                    },
                    {
                        name: "Page",
                        value: "page"
                    },
                    {
                        name: "PageSize",
                        value: "pageSize"
                    },
                    {
                        name: "ProspectOutcome",
                        value: "prospectOutcome"
                    },
                    {
                        name: "Search",
                        value: "search"
                    },
                    {
                        name: "SequenceId",
                        value: "sequenceId"
                    },
                    {
                        name: "SortBy",
                        value: "sortBy"
                    },
                    {
                        name: "SortOrder",
                        value: "sortOrder"
                    },
                    {
                        name: "Status",
                        value: "status"
                    },
                    {
                        name: "TaskAssignee",
                        value: "taskAssignee"
                    },
                    {
                        name: "TaskType",
                        value: "taskType"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_getTasks"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Task ID",
                name: "taskId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_skipTask"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_skipTask"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Task ID",
                name: "taskId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_snoozeTask"
                        ]
                    }
                }
            },
            {
                displayName: "Snooze Until",
                name: "snoozeUntil",
                type: "string",
                default: "",
                required: true,
                description: "Date and time until which the task should be snoozed",
                placeholder: "e.g. 2024-06-17T05:55:38.434Z",
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_snoozeTask"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_snoozeTask"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Call Outcome",
                        name: "callOutcome",
                        type: "options",
                        default: "Interested",
                        description: "Call outcome for call tasks (optional)",
                        placeholder: "e.g. no-answer",
                        options: [
                            {
                                name: "Callback Later",
                                value: "Callback Later"
                            },
                            {
                                name: "Followup",
                                value: "Followup"
                            },
                            {
                                name: "Interested",
                                value: "Interested"
                            },
                            {
                                name: "Left Voicemail",
                                value: "Left Voicemail"
                            },
                            {
                                name: "No Answer",
                                value: "No Answer"
                            },
                            {
                                name: "Not In Service",
                                value: "Not In Service"
                            },
                            {
                                name: "Not Interested",
                                value: "Not Interested"
                            },
                            {
                                name: "Wrong Number",
                                value: "Wrong Number"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_snoozeTask"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Task ID",
                name: "taskId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_updateTaskNote"
                        ]
                    }
                }
            },
            {
                displayName: "Note",
                name: "note",
                type: "string",
                default: "",
                required: true,
                description: "Note content for the task",
                placeholder: "e.g. Mention the new product updates in the call",
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_updateTaskNote"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tasks"
                        ],
                        operation: [
                            "TaskController_updateTaskNote"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "unibox"
                        ]
                    }
                },
                default: "UniboxController_getCategories",
                options: [
                    {
                        name: "List Email Reply Categories",
                        value: "UniboxController_getCategories",
                        action: "List email reply categories unibox",
                        description: "Returns the list of reply categories used to classify inbound emails in the unified inbox (unibox). each category has a unique key, a display name, a sentiment (positive, negative, or neutral), and a flag indicating whether it is a system default category or a custom one."
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "unibox"
                        ],
                        operation: [
                            "UniboxController_getCategories"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ]
                    }
                },
                default: "UnifiedInboxController_getAll",
                options: [
                    {
                        name: "Get An Email Thread",
                        value: "UnifiedInboxController_getAllEmails",
                        action: "Get email thread unified inbox",
                        description: "Return all emails in the specified thread. unified inbox."
                    },
                    {
                        name: "Get Email Content",
                        value: "UnifiedInboxController_getEmailContentForSingleEmail",
                        action: "Get email content unified inbox",
                        description: "Return the content of the specified email in a thread. unified inbox."
                    },
                    {
                        name: "Get The Unread Thread Count",
                        value: "UnifiedInboxController_getAll",
                        action: "Get unread thread count unified inbox",
                        description: "Return the number of unread email threads. unified inbox."
                    },
                    {
                        name: "List Inbox Emails",
                        value: "UnifiedInboxController_getEmailList",
                        action: "List inbox emails unified inbox",
                        description: "List inbox emails matching the supplied search, category, sentiment, date, owner, and account filters. unified inbox."
                    },
                    {
                        name: "List Inbox Outcomes",
                        value: "UnifiedInboxController_getFields",
                        action: "List inbox outcomes unified inbox",
                        description: "List inbox outcomes, optionally filtered by category. unified inbox."
                    },
                    {
                        name: "Reply To An Email Thread",
                        value: "UnifiedInboxController_replyOnEmail",
                        action: "Reply to an email thread unified inbox",
                        description: "Send a reply in the specified email thread, with optional cc, bcc, and attachments. unified inbox."
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getAll"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Email Thread ID",
                name: "emailThreadId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getAllEmails"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getAllEmails"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Email Thread ID",
                name: "emailThreadId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getEmailContentForSingleEmail"
                        ]
                    }
                }
            },
            {
                displayName: "Email ID",
                name: "emailId",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getEmailContentForSingleEmail"
                        ]
                    }
                }
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getEmailContentForSingleEmail"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Limit",
                name: "limit",
                type: "number",
                default: 50,
                required: true,
                description: "Max number of results to return",
                typeOptions: {
                    minValue: 1
                },
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getEmailList"
                        ]
                    }
                }
            },
            {
                displayName: "Owners",
                name: "owners",
                type: "json",
                default: [],
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getEmailList"
                        ]
                    }
                }
            },
            {
                displayName: "Page",
                name: "page",
                type: "number",
                default: 0,
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getEmailList"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getEmailList"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Category IDs",
                        name: "categoryIds",
                        type: "json",
                        default: []
                    },
                    {
                        displayName: "Client IDs",
                        name: "clientIds",
                        type: "json",
                        default: []
                    },
                    {
                        displayName: "Email Account IDs",
                        name: "emailAccountIds",
                        type: "json",
                        default: []
                    },
                    {
                        displayName: "End Date",
                        name: "endDate",
                        type: "string",
                        default: ""
                    },
                    {
                        displayName: "Is Read",
                        name: "isRead",
                        type: "number",
                        default: 0
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: ""
                    },
                    {
                        displayName: "Sentiment",
                        name: "sentiment",
                        type: "options",
                        default: "Positive",
                        description: "Filter by sentimentstype",
                        options: [
                            {
                                name: "Negative",
                                value: "Negative"
                            },
                            {
                                name: "Neutral",
                                value: "Neutral"
                            },
                            {
                                name: "Positive",
                                value: "Positive"
                            },
                            {
                                name: "Uncategorized",
                                value: "Uncategorized"
                            }
                        ]
                    },
                    {
                        displayName: "Sequence IDs",
                        name: "sequenceIds",
                        type: "json",
                        default: []
                    },
                    {
                        displayName: "Start Date",
                        name: "startDate",
                        type: "string",
                        default: ""
                    },
                    {
                        displayName: "Type",
                        name: "type",
                        type: "options",
                        default: "system",
                        description: "Filter by emails type",
                        options: [
                            {
                                name: "External",
                                value: "external"
                            },
                            {
                                name: "System",
                                value: "system"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getEmailList"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Category",
                name: "category",
                type: "options",
                default: "custom",
                required: true,
                description: "Select outcome category",
                placeholder: "e.g. default",
                options: [
                    {
                        name: "Custom",
                        value: "custom"
                    },
                    {
                        name: "Default",
                        value: "default"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getFields"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getFields"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 1,
                        description: "Page number for pagination",
                        placeholder: "e.g. 1"
                    },
                    {
                        displayName: "Page Size",
                        name: "pageSize",
                        type: "number",
                        default: 25,
                        description: "Number of items per page. maximum pagesize is 100.",
                        placeholder: "e.g. 25"
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_getFields"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            },
            {
                displayName: "Content",
                name: "content",
                type: "string",
                default: "",
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_replyOnEmail"
                        ]
                    }
                }
            },
            {
                displayName: "Email ID",
                name: "emailId",
                type: "string",
                default: "",
                required: true,
                description: "Hashed email ID from the unified inbox thread response",
                placeholder: "e.g. EaxmPl3Id",
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_replyOnEmail"
                        ]
                    }
                }
            },
            {
                displayName: "Email Thread ID",
                name: "emailThreadId",
                type: "string",
                default: "",
                required: true,
                description: "Hashed unified inbox thread ID. for webhook flows, use conversationid from the webhook payload as this value.",
                placeholder: "e.g. Rxh247rdgp",
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_replyOnEmail"
                        ]
                    }
                }
            },
            {
                displayName: "Template ID",
                name: "templateId",
                type: "number",
                default: 0,
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_replyOnEmail"
                        ]
                    }
                }
            },
            {
                displayName: "To",
                name: "to",
                type: "json",
                default: [],
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_replyOnEmail"
                        ]
                    }
                }
            },
            {
                displayName: "Unified Scheduled ID",
                name: "unifiedScheduledId",
                type: "number",
                default: 0,
                required: true,
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_replyOnEmail"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_replyOnEmail"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Attachment IDs",
                        name: "attachmentIds",
                        type: "json",
                        default: [],
                        description: "Hashed attachment ID(s) returned by post /v1/attachments",
                        placeholder: "e.g. Y8aLkbK0PN"
                    },
                    {
                        displayName: "Bcc",
                        name: "bcc",
                        type: "json",
                        default: []
                    },
                    {
                        displayName: "Cc",
                        name: "cc",
                        type: "json",
                        default: []
                    }
                ]
            },
            {
                displayName: "Options",
                name: "options",
                type: "collection",
                placeholder: "Add Option",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "unifiedInbox"
                        ],
                        operation: [
                            "UnifiedInboxController_replyOnEmail"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Destination URL",
                        name: "server_selfHosted_baseUrl",
                        type: "string",
                        default: "https://api.example.com",
                        description: "HTTPS destination URL for the API",
                        placeholder: "https://api.example.com",
                        validateType: "url"
                    }
                ]
            }
        ]
    };

  public async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const inputItems = this.getInputData();
    const output: INodeExecutionData[] = [];
    for (let itemIndex = 0; itemIndex < inputItems.length; itemIndex += 1) {
      const outputStart = output.length;
      let errorPlan: Record<string, { title: string; recovery?: string; parameter?: string }> = {};
      try {
        const operation = this.getNodeParameter('operation', itemIndex) as string;
        const nodeVersion = this.getNode().typeVersion;
        let additionalFields: IDataObject = {};
        const nodeOptions = this.getNodeParameter('options', itemIndex, {}) as IDataObject;
        
        let retryContract: RetryContract = { mode: 'none', maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0 };
        let credentialApplications: CredentialApplication[] | undefined;
        let options: IHttpRequestOptions;
        let pagination: PaginationContract = { style: 'none', advancement: '', maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10 * 1024 * 1024, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        let responsePlan: { binary: boolean; full: boolean; envelopePath: string; itemPath: string; fields: string[]; simplified: string[] } = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        switch (operation) {
          case "AnalyticsController_exportEmailSentDetails": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/analytics/email-sent-details";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"endDate","displayName":"End Date","description":"End date for the report (ISO date string)","type":"string","required":true,"example":"2024-01-31"}, this.getNodeParameter("endDate", itemIndex), this, itemIndex);
    if (additionalFields["sequenceIds"] !== undefined) setBodyField(body as IDataObject, {"name":"sequenceIds","displayName":"Sequence Ids","description":"Filter by specific sequence IDs","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["sequenceIds"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"startDate","displayName":"Start Date","description":"Start date for the report (ISO date string)","type":"string","required":true,"example":"2024-01-01"}, this.getNodeParameter("startDate", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "AnalyticsController_getAllTeamSummarizedStats": {
        
        
        const path = "/v1/analytics/team/stats";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"countBy","displayName":"Count By","description":"How to group the statistics (relative or absolute)","type":"string","required":true,"enum":["relative","absolute"],"example":"absolute"}, this.getNodeParameter("countBy", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"endDate","displayName":"End Date","description":"End date for the report period in ISO 8601 format with timezone","type":"string","required":true,"example":"2024-01-31T23:59:59.999+00:00"}, this.getNodeParameter("endDate", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"limit","displayName":"Limit","description":"Maximum number of records to return per page","type":"number","required":true,"minValue":1,"example":10}, this.getNodeParameter("limit", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"orderBy","displayName":"Order By","description":"Field name to order the results by. Valid values depend on report type","type":"string","required":true,"enum":["prospectAdded","prospectContacted","opened","clicked","replied","unSubscribed","bounced","Uncategorized","Interested","Not Interested","Meeting Booked","Out of Office","Closed","Not Now","Do Not Contact","prospectContacted","emailSent","opened","clicked","replied","bounced"],"example":"prospectAdded"}, this.getNodeParameter("orderBy", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"page","displayName":"Page","description":"Page number for pagination","type":"number","required":true,"minValue":1,"example":1}, this.getNodeParameter("page", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"sortOrder","displayName":"Sort Order","description":"Sort direction for ordering results","type":"string","required":true,"enum":["asc","desc"],"default":"desc","example":"desc"}, this.getNodeParameter("sortOrder", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"startDate","displayName":"Start Date","description":"Start date for the report period in ISO 8601 format with timezone","type":"string","required":true,"example":"2024-01-01T00:00:00.000+00:00"}, this.getNodeParameter("startDate", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"type","displayName":"Type","description":"Type of report to generate (prospect or email)","type":"string","required":true,"enum":["prospect","email"],"example":"prospect"}, this.getNodeParameter("type", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"userIds","displayName":"User Ids","description":"Array of encrypted user IDs to filter the report","type":"array","required":true,"example":["abc123xyz","def456uvw"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("userIds", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {};
        break;
      }
    case "AnalyticsController_getEmailAccountStats": {
        
        
        const path = "/v1/analytics/emailaccount/stats";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"emailId","displayName":"Email Id","description":"Id of the email account","type":"string","required":true,"example":"2dP27N0gZ4"}, this.getNodeParameter("emailId", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "AnalyticsController_getSequenceConsolidatedReport": {
        
        
        const path = "/v1/analytics/consolidated-stats";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"endDate","displayName":"End Date","description":"End Date must in a valid date & it must not exceed 1 year after Start Date. Example:- 2001-12-18 (YYYY-MM-DD)","type":"string","required":true,"example":"2024-11-19"}, this.getNodeParameter("endDate", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"pageLimit","displayName":"Page Limit","description":"Number of documents to be returned. Page limit should be between 10 & 500 per page.","type":"number","required":true,"example":25}, this.getNodeParameter("pageLimit", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"pageNum","displayName":"Page Num","description":"Page number. It should be greater then 1","type":"number","required":true,"example":1}, this.getNodeParameter("pageNum", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"sequenceIds","displayName":"Sequence Ids","description":"Ids of the sequences","type":"array","required":true,"example":["2dP27N0gZ4","2dP27NugZ3","2dP27NrgZ3"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("sequenceIds", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"startDate","displayName":"Start Date","description":"Start Date must in a valid date & it must not be older than 2 years ago. Example:- 2001-12-18 (YYYY-MM-DD).","type":"string","required":true,"example":"2024-11-15"}, this.getNodeParameter("startDate", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "AnalyticsController_getSequenceStats": {
        
        
        const path = "/v1/analytics/stats";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"sequenceId","displayName":"Sequence Id","description":"Id of the sequence","type":"string","required":true,"example":"2dP27N0gZ4"}, this.getNodeParameter("sequenceId", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "AttachmentController_uploadAttachment": {
        
        
        const path = "/v1/attachments";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"file","displayName":"File","type":"string","format":"binary","required":true}, this.getNodeParameter("file", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: toFormData(body), json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to upload attachment<br><br>"}};
        break;
      }
    case "DomainBlacklistController_addBlacklistDomain": {
        
        
        const path = "/v1/blacklist-domains";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"domains","displayName":"Domains","description":"List of domain names to be blacklisted seperated by ',' (not prefixed by http:// or https://). \n      <br>Maximum domain limit is 50 & Minumum domain limit is 1.","type":"string","required":true,"example":"example.com,dev.to,rataalada.com"}, this.getNodeParameter("domains", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"40001: Enter the valid domain name in the format domain.com that does not already exist in the blacklist<br><br>40002: Domain already exist<br><br>40003: The maximum domain limit has been exceeded. You can blacklist up to 50 domains at a time<br><br>40004: Could not add domains to blacklist<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "ClientController_assignResource": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/clients/assign/{resourceType}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{resourceType}").join(encodeURIComponent(String(this.getNodeParameter("resourceType", itemIndex))));
        if (additionalFields["clientId"] !== undefined) setBodyField(body as IDataObject, {"name":"clientId","displayName":"Client Id","description":"Optional client ID for scoping","type":"string","example":"9pa8BRwy2"}, additionalFields["clientId"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"resourceIds","displayName":"Resource Ids","description":"IDs of resources to assign","type":"array","required":true,"example":["1Gz3xlNwr9","ajzR8xpPAq"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("resourceIds", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to assign resource to client<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "ClientController_createClient": {
        
        
        const path = "/v1/clients";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"companyName","displayName":"Company Name","description":"Client's company name.","type":"string","required":true,"example":"Acme Growth"}, this.getNodeParameter("companyName", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"email","displayName":"Email","description":"Client's email address. Must be a valid email on a non-blacklisted domain.","type":"string","required":true,"example":"maya.client@example.org"}, this.getNodeParameter("email", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"firstName","displayName":"First Name","description":"Client's first name.","type":"string","required":true,"example":"Maya"}, this.getNodeParameter("firstName", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"lastName","displayName":"Last Name","description":"Client's last name.","type":"string","required":true,"example":"Client"}, this.getNodeParameter("lastName", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"permission","displayName":"Permission","description":"Access level for the client. Accepted values: \"1\" = Full access, \"2\" = Limited access.","type":"string","required":true,"enum":["1","2"],"example":"1"}, this.getNodeParameter("permission", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Same client user already exists<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "ClientController_getClientLists": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/clients";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
    if (additionalFields["pageSize"] !== undefined) qs["pageSize"] = additionalFields["pageSize"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    qs["sort"] = this.getNodeParameter("sort", itemIndex);
    qs["sortBy"] = this.getNodeParameter("sortBy", itemIndex);
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"40004: Failed to fetch clients<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "DncController_addItemsToDncList": {
        
        
        const path = "/v1/dnc";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"dncListId","displayName":"Dnc List Id","description":"DNC list id to which the items should be added. Use the string `id` returned by GET /v1/dnc (e.g. \"vWjzRkZwAq\") — do not coerce it to a number.","type":"string","required":true,"example":"vWjzRkZwAq"}, this.getNodeParameter("dncListId", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"items","displayName":"Items","description":"Array of strings containing either emails or domains","type":"array","required":true,"example":["example@example.com","domain.com"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("items", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {};
        break;
      }
    case "DncController_getDncById": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/dnc/{dncListId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{dncListId}").join(encodeURIComponent(String(this.getNodeParameter("dncListId", itemIndex))));
    if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
    if (additionalFields["pageSize"] !== undefined) qs["pageSize"] = additionalFields["pageSize"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    qs["type"] = this.getNodeParameter("type", itemIndex);
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"40004: Failed to fetch dnc item<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "DncController_getDncListWithItem": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/dnc/item/search";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
    if (additionalFields["pageSize"] !== undefined) qs["pageSize"] = additionalFields["pageSize"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    qs["type"] = this.getNodeParameter("type", itemIndex);
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"40004: Failed to fetch dnc item<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "DncController_getDncLists": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/dnc";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
    if (additionalFields["pageSize"] !== undefined) qs["pageSize"] = additionalFields["pageSize"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    qs["sort"] = this.getNodeParameter("sort", itemIndex);
    qs["sortBy"] = this.getNodeParameter("sortBy", itemIndex);
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"40004: Failed to fetch dnc list<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "DomainController_deleteDomain": {
        
        
        let path = "/v1/domain/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to delete domain<br><br>1001: Invalid domain id<br><br>"}};
        break;
      }
    case "DomainController_generateMailboxNames": {
        
        
        const path = "/v1/domain/generate-mailbox-names";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"count","displayName":"Count","description":"Number of suggestions to return. Min 1, max 100.","type":"number","required":true,"minValue":1,"maxValue":100,"example":5}, this.getNodeParameter("count", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"domain","displayName":"Domain","description":"The domain to generate addresses for (e.g. \"tryoutreachteam.com\").","type":"string","required":true,"example":"tryoutreachteam.com"}, this.getNodeParameter("domain", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"firstName","displayName":"First Name","description":"Sender's first name.","type":"string","required":true,"example":"John"}, this.getNodeParameter("firstName", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"lastName","displayName":"Last Name","description":"Sender's last name.","type":"string","required":true,"example":"Smith"}, this.getNodeParameter("lastName", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to generate mailbox names<br><br>"}};
        break;
      }
    case "DomainController_getDomainPlans": {
        
        
        const path = "/v1/domain/plans";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to fetch domain plans<br><br>"}};
        break;
      }
    case "DomainController_getOrderStatus": {
        
        
        let path = "/v1/domain/orders/{orderId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{orderId}").join(encodeURIComponent(String(this.getNodeParameter("orderId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to fetch order status<br><br>1001: Invalid order id<br><br>"}};
        break;
      }
    case "DomainController_listDomainOrders": {
        
        
        const path = "/v1/domain/orders";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to fetch domain orders<br><br>"}};
        break;
      }
    case "DomainController_listDomains": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/domain";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
    if (additionalFields["order"] !== undefined) qs["order"] = additionalFields["order"];
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to fetch domains<br><br>"}};
        break;
      }
    case "DomainController_purchaseDomain": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/domain";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"address","displayName":"Address","description":"Registrant street address.","type":"string","required":true,"example":"123 Market Street"}, this.getNodeParameter("address", itemIndex), this, itemIndex);
    if (additionalFields["city"] !== undefined) setBodyField(body as IDataObject, {"name":"city","displayName":"City","description":"Registrant city.","type":"string","example":"San Francisco"}, additionalFields["city"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"companyName","displayName":"Company Name","description":"Registrant organisation name.","type":"string","required":true,"example":"Acme Corp"}, this.getNodeParameter("companyName", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"country","displayName":"Country","description":"Registrant country. ISO alpha-2 code (e.g. \"US\", \"IN\").","type":"string","required":true,"example":"US"}, this.getNodeParameter("country", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"domainOwnerFirstname","displayName":"Domain Owner Firstname","description":"Registrant first name. 1–50 characters.","type":"string","required":true,"example":"John"}, this.getNodeParameter("domainOwnerFirstname", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"domainOwnerLastname","displayName":"Domain Owner Lastname","description":"Registrant last name. 1–50 characters.","type":"string","required":true,"example":"Smith"}, this.getNodeParameter("domainOwnerLastname", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"domains","displayName":"Domains","description":"One entry per domain to purchase. Minimum 1.","type":"array","required":true,"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"mailboxes","displayName":"Mailboxes","description":"One entry per mailbox to create on this domain. Minimum 1.","type":"array","required":true,"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"email","displayName":"Email","description":"Full email address to create (e.g. \"john.smith@tryoutreachteam.com\"). Must use the parent domain.","type":"string","required":true,"example":"john.smith@tryoutreachteam.com"},{"name":"firstname","displayName":"Firstname","description":"Mailbox owner's first name (used as sender display name). 1–50 chars. Note: field is 'firstname' (lowercase 'n').","type":"string","required":true,"example":"John"},{"name":"lastname","displayName":"Lastname","description":"Mailbox owner's last name. 1–50 chars.","type":"string","required":true,"example":"Smith"},{"name":"profilePicId","displayName":"Profile Pic Id","description":"Numeric ID of a pre-uploaded profile picture from POST /v1/domain/profile-picture/upload. Pass 0 for no picture.","type":"number","required":true,"minValue":0,"example":0}]}},{"name":"name","displayName":"Name","description":"Domain name to register (e.g. \"tryoutreachteam.com\"). 3–254 chars. No protocol prefix.","type":"string","required":true,"example":"tryoutreachteam.com"},{"name":"redirectUrl","displayName":"Redirect Url","description":"Forward Domain — bare domain only, no http:// or https:// prefix. HTTP visitors are forwarded here. 3–254 chars.","type":"string","required":true,"example":"acmecorp.com"}]}}, this.getNodeParameter("domains", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"emailServiceProvider","displayName":"Email Service Provider","description":"The provider used to provision mailboxes. Determines pricing and infrastructure.","type":"string","required":true,"enum":["Google","Microsoft","Azure"],"example":"Google"}, this.getNodeParameter("emailServiceProvider", itemIndex), this, itemIndex);
    if (additionalFields["isFreeOrder"] !== undefined) setBodyField(body as IDataObject, {"name":"isFreeOrder","displayName":"Is Free Order","description":"Set true to use free-tier domain/mailbox credits on the account.","type":"boolean","example":false}, additionalFields["isFreeOrder"], this, itemIndex);
    if (additionalFields["isReplacement"] !== undefined) setBodyField(body as IDataObject, {"name":"isReplacement","displayName":"Is Replacement","description":"Set true to fulfil this order using replacement-slot credits from a previously cancelled or expired order.","type":"boolean","example":false}, additionalFields["isReplacement"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"phoneNumber","displayName":"Phone Number","description":"Registrant phone in E.164 format — + + country code + number (e.g. \"+14155551234\").","type":"string","required":true,"example":"+14155551234"}, this.getNodeParameter("phoneNumber", itemIndex), this, itemIndex);
    if (additionalFields["planType"] !== undefined) setBodyField(body as IDataObject, {"name":"planType","displayName":"Plan Type","description":"Billing cycle for mailbox subscriptions. \"quarterly\" (default), \"annualFlexi\", or \"annualFixed\". NOTE: Azure only supports \"quarterly\" — passing any other value returns a 400 error.","type":"string","enum":["quarterly","annualFlexi","annualFixed"],"example":"quarterly"}, additionalFields["planType"], this, itemIndex);
    if (additionalFields["state"] !== undefined) setBodyField(body as IDataObject, {"name":"state","displayName":"State","description":"Registrant state or province.","type":"string","example":"CA"}, additionalFields["state"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"zipcode","displayName":"Zipcode","description":"Postal code. Must be valid for the given country.","type":"string","required":true,"example":"94105"}, this.getNodeParameter("zipcode", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to purchase domain<br><br>"}};
        break;
      }
    case "DomainController_revokeDomain": {
        
        
        let path = "/v1/domain/{id}/revoke";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to revoke domain<br><br>1001: Invalid domain id<br><br>"}};
        break;
      }
    case "DomainController_searchDomains": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/domain/search";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        qs["domain"] = this.getNodeParameter("domain", itemIndex);
    qs["emailServiceProvider"] = this.getNodeParameter("emailServiceProvider", itemIndex);
    if (additionalFields["tld"] !== undefined) qs["tld"] = additionalFields["tld"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to search domains<br><br>"}};
        break;
      }
    case "DomainController_uploadProfilePicture": {
        
        
        const path = "/v1/domain/profile-picture/upload";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"file","displayName":"File","type":"string","format":"binary","required":true}, this.getNodeParameter("file", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: toFormData(body), json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to upload profile picture<br><br>"},"415":{"title":"Invalid file type (not PNG/JPG/GIF) or file exceeds 1 MB"}};
        break;
      }
    case "EmailAccountController_addEmailAccounts": {
        
        
        const path = "/v1/email-accounts/connect";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        let body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        body = normalizeJsonValue(this.getNodeParameter("bodyJson", itemIndex), "Body JSON", this, itemIndex) as typeof body; validateBodyValue(body, {"name":"bodyJson","displayName":"Body JSON","type":"any","required":true,"description":"Raw request body","items":{"name":"item","displayName":"Item","type":"object","fields":[{"name":"emailServiceProvider","displayName":"Email Service Provider","type":"string","required":true,"description":"Email service provider : gsuite, microsoft, o365, yahoo, zoho, godaddy, yandex, sendgrid, other","enum":["gsuite","microsoft","o365","yahoo","zoho","godaddy","yandex","sendgrid","other"],"example":"gsuite"},{"name":"fromFirstName","displayName":"From First Name","type":"string","required":true,"description":"First name of the sender","example":"John"},{"name":"fromLastName","displayName":"From Last Name","type":"string","required":true,"description":"Last name of the sender","example":"Snow"},{"name":"imapEmailAddress","displayName":"Imap Email Address","type":"string","required":true,"description":"IMAP email address","example":"john@example.com"},{"name":"imapEncryption","displayName":"Imap Encryption","type":"string","required":true,"description":"IMAP encryption type (SSL/TLS/NONE)","enum":["ssl","tls","none"],"example":"ssl"},{"name":"imapHost","displayName":"Imap Host","type":"string","required":true,"description":"IMAP host","example":"imap.gmail.com"},{"name":"imapPassword","displayName":"Imap Password","type":"string","required":true,"description":"IMAP password","example":"imap-password"},{"name":"imapPort","displayName":"Imap Port","type":"number","required":true,"description":"IMAP port","example":993},{"name":"imapUserName","displayName":"Imap User Name","type":"string","required":true,"description":"IMAP username","example":"john@example.com"},{"name":"smtpEmailAddress","displayName":"Smtp Email Address","type":"string","required":true,"description":"SMTP email address","example":"john@example.com"},{"name":"smtpEncryption","displayName":"Smtp Encryption","type":"string","required":true,"description":"SMTP encryption type (SSL/TLS/NONE)","enum":["ssl","tls","none"],"example":"ssl"},{"name":"smtpHost","displayName":"Smtp Host","type":"string","required":true,"description":"SMTP host","example":"smtp.gmail.com"},{"name":"smtpPassword","displayName":"Smtp Password","type":"string","required":true,"description":"SMTP password","example":"smtp-password"},{"name":"smtpPort","displayName":"Smtp Port","type":"number","required":true,"description":"SMTP port","example":465},{"name":"smtpUserName","displayName":"Smtp User Name","type":"string","required":true,"description":"SMTP username","example":"john@example.com"}],"representation":"raw"},"representation":"raw"}, "Body JSON", this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "EmailAccountController_connectEmailAccount": {
        
        
        const path = "/v1/email-accounts/smtp-imap/connect";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"emailServiceProvider","displayName":"Email Service Provider","description":"Email service provider identifier. Accepted values: gmail, microsoft, gsuite, o365, yahoo, zoho, godaddy, yandex, sendgrid, other.","type":"string","required":true,"enum":["gmail","microsoft","gsuite","o365","yahoo","zoho","godaddy","yandex","sendgrid","other"],"example":"zoho"}, this.getNodeParameter("emailServiceProvider", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"fromFirstName","displayName":"From First Name","description":"Sender's first name.","type":"string","required":true,"example":"Sarah"}, this.getNodeParameter("fromFirstName", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"fromLastName","displayName":"From Last Name","description":"Sender's last name.","type":"string","required":true,"example":"Chen"}, this.getNodeParameter("fromLastName", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"fromName","displayName":"From Name","description":"Full display name shown as the sender.","type":"string","required":true,"example":"Sarah Chen"}, this.getNodeParameter("fromName", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"payload","displayName":"Payload","description":"SMTP + IMAP connection settings.","type":"object","required":true,"representation":"raw","fields":[{"name":"imap","displayName":"Imap","description":"IMAP connection settings.","type":"object","required":true,"representation":"raw","fields":[{"name":"emailAddress","displayName":"Email Address","description":"Email address used for IMAP authentication.","type":"string","required":true,"example":"sarah.chen@outreach.io"},{"name":"encryption","displayName":"Encryption","description":"IMAP connection encryption.","type":"string","required":true,"enum":["TLS","SSL"],"example":"SSL"},{"name":"host","displayName":"Host","description":"IMAP hostname.","type":"string","required":true,"example":"imap.zoho.com"},{"name":"password","displayName":"Password","description":"IMAP password or app-specific password.","type":"string","required":true,"example":"AppPassword123"},{"name":"port","displayName":"Port","description":"IMAP port (1–65535).","type":"number","required":true,"minValue":1,"maxValue":65535,"example":993}]},{"name":"smtp","displayName":"Smtp","description":"SMTP connection settings.","type":"object","required":true,"representation":"raw","fields":[{"name":"emailAddress","displayName":"Email Address","description":"Email address used for SMTP authentication.","type":"string","required":true,"example":"sarah.chen@outreach.io"},{"name":"encryption","displayName":"Encryption","description":"SMTP connection encryption.","type":"string","required":true,"enum":["TLS","SSL"],"example":"TLS"},{"name":"host","displayName":"Host","description":"SMTP hostname.","type":"string","required":true,"example":"smtp.zoho.com"},{"name":"password","displayName":"Password","description":"SMTP password or app-specific password.","type":"string","required":true,"example":"AppPassword123"},{"name":"port","displayName":"Port","description":"SMTP port (1–65535).","type":"number","required":true,"minValue":1,"maxValue":65535,"example":587},{"name":"userName","displayName":"User Name","description":"SMTP username — usually the same as the email address.","type":"string","required":true,"example":"sarah.chen@outreach.io"}]}]}, this.getNodeParameter("payload", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to connect email account<br><br>"},"409":{"title":"1001: Email account already exists<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "EmailAccountController_getEmailAccounts": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/email-accounts";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["addedBy"] !== undefined) setBodyField(body as IDataObject, {"name":"addedBy","displayName":"Added By","description":"List of user IDs who added the email accounts.","type":"array","example":["JA5YdAr9wy"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"array","representation":"raw"}}, additionalFields["addedBy"], this, itemIndex);
    if (additionalFields["clientIds"] !== undefined) setBodyField(body as IDataObject, {"name":"clientIds","displayName":"Client Ids","description":"Array of email account IDs associated with the user.","type":"array","example":["JA5YdAr9wy"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"array","representation":"raw"}}, additionalFields["clientIds"], this, itemIndex);
    if (additionalFields["emailServiceProvider"] !== undefined) setBodyField(body as IDataObject, {"name":"emailServiceProvider","displayName":"Email Service Provider","description":"List of email service providers associated with the email accounts.","type":"array","example":["gsuite","microsoft"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["emailServiceProvider"], this, itemIndex);
    if (additionalFields["page"] !== undefined) setBodyField(body as IDataObject, {"name":"page","displayName":"Page","description":"Page number for pagination. Defaults to the first page.","type":"number","default":1,"example":1}, additionalFields["page"], this, itemIndex);
    if (additionalFields["pageSize"] !== undefined) setBodyField(body as IDataObject, {"name":"pageSize","displayName":"Page Size","description":"Number of items per page. Maximum pageSize is 100.","type":"number","default":25,"example":25}, additionalFields["pageSize"], this, itemIndex);
    if (additionalFields["search"] !== undefined) setBodyField(body as IDataObject, {"name":"search","displayName":"Search","description":"Search term for filtering email accounts based on email or firstName or lastName","type":"string","example":"john.doe@example.com"}, additionalFields["search"], this, itemIndex);
    if (additionalFields["sequenceIds"] !== undefined) setBodyField(body as IDataObject, {"name":"sequenceIds","displayName":"Sequence Ids","description":"List of sequence IDs to filter the email accounts.","type":"array","example":["JA5YdAr9wy"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"array","representation":"raw"}}, additionalFields["sequenceIds"], this, itemIndex);
    if (additionalFields["sort"] !== undefined) setBodyField(body as IDataObject, {"name":"sort","displayName":"Sort","description":"Sort order for the results, either ascending or descending.","type":"string","enum":["ASC","DESC"],"example":"DESC"}, additionalFields["sort"], this, itemIndex);
    if (additionalFields["sortByKey"] !== undefined) setBodyField(body as IDataObject, {"name":"sortByKey","displayName":"Sort By Key","description":"Sort key to order the email accounts list. Defaults to creation date.","type":"string","enum":["created-date","health-score","remaining-quota","clientFirstName"],"example":"health-score"}, additionalFields["sortByKey"], this, itemIndex);
    if (additionalFields["status"] !== undefined) setBodyField(body as IDataObject, {"name":"status","displayName":"Status","description":"Filter email accounts by status. 0 for Inactive, 1 for Active, 2 for Suspended.","type":"number","example":1}, additionalFields["status"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["connectedEmailsCount","disconnectedEmailsCount","emails","meta"], simplified: ["connectedEmailsCount","disconnectedEmailsCount","emails","meta"] };
        errorPlan = {"400":{"title":"Error codes: 40107 invalid sequence input; 40202 invalid client input; 40203 invalid addedBy input; 40204 invalid emailServiceProvider input."},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "EmailAccountController_getEmailAccountsConnectStatus": {
        
        
        let path = "/v1/email-accounts/connect/status/{requestId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{requestId}").join(encodeURIComponent(String(this.getNodeParameter("requestId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"40000: Email Account connect request not found<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "EmailAccountController_reconnectEmailAccounts": {
        
        
        const path = "/v1/email-accounts/reconnect";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"emailAccountIds","displayName":"Email Account Ids","description":"Array of email account IDs to be toggled on.","type":"array","required":true,"example":["1Gz3xlNwr9","ajzR8xpPAq","vXwAZr6P8q"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"number"}}, this.getNodeParameter("emailAccountIds", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "EmailAccountController_updateEmailAccounts": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/email-accounts/bulk-update";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["bcc"] !== undefined) setBodyField(body as IDataObject, {"name":"bcc","displayName":"Bcc","description":"BCC email addresses (comma-separated if multiple)","type":"string","example":"bcc@example.com"}, additionalFields["bcc"], this, itemIndex);
    if (additionalFields["clientId"] !== undefined) setBodyField(body as IDataObject, {"name":"clientId","displayName":"Client Id","description":"client id associated with the user.","type":"number","example":"JA5YdAr9wy"}, additionalFields["clientId"], this, itemIndex);
    if (additionalFields["dailyQuota"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyQuota","displayName":"Daily Quota","description":"Daily sending quota for the email account","type":"number","example":500}, additionalFields["dailyQuota"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"emailAccountIds","displayName":"Email Account Ids","description":"Array of email account IDs associated with the user","type":"array","required":true,"example":["Z6zxEXJwAk","D6zxEXJwAk"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"number"}}, this.getNodeParameter("emailAccountIds", itemIndex), this, itemIndex);
    if (additionalFields["rampUpInitialSendingLimit"] !== undefined) setBodyField(body as IDataObject, {"name":"rampUpInitialSendingLimit","displayName":"Ramp Up Initial Sending Limit","description":"Initial sending limit for ramp-up","type":"number","example":100}, additionalFields["rampUpInitialSendingLimit"], this, itemIndex);
    if (additionalFields["rampUpPercent"] !== undefined) setBodyField(body as IDataObject, {"name":"rampUpPercent","displayName":"Ramp Up Percent","description":"Ramp-up percentage increase per interval","type":"number","example":10}, additionalFields["rampUpPercent"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"senderFirstName","displayName":"Sender First Name","description":"First name of the sender (required)","type":"string","required":true,"example":"John"}, this.getNodeParameter("senderFirstName", itemIndex), this, itemIndex);
    if (additionalFields["senderLastName"] !== undefined) setBodyField(body as IDataObject, {"name":"senderLastName","displayName":"Sender Last Name","description":"Last name of the sender","type":"string","example":"Doe"}, additionalFields["senderLastName"], this, itemIndex);
    if (additionalFields["sendingIntervalMax"] !== undefined) setBodyField(body as IDataObject, {"name":"sendingIntervalMax","displayName":"Sending Interval Max","description":"Maximum interval (in seconds) between email sends","type":"number","example":120}, additionalFields["sendingIntervalMax"], this, itemIndex);
    if (additionalFields["sendingIntervalMin"] !== undefined) setBodyField(body as IDataObject, {"name":"sendingIntervalMin","displayName":"Sending Interval Min","description":"Minimum interval (in seconds) between email sends","type":"number","example":60}, additionalFields["sendingIntervalMin"], this, itemIndex);
    if (additionalFields["signatureHtml"] !== undefined) setBodyField(body as IDataObject, {"name":"signatureHtml","displayName":"Signature Html","description":"HTML signature for the email","type":"string","example":"<p>Best regards,<br>John Doe</p>"}, additionalFields["signatureHtml"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"<b><i> Error Codes : </i></b><br><br>\n>* <b>Status:</b> 40202 &nbsp;&nbsp;<b>Description:</b> The client input is invalid. Please check and try again.\n\n>* <b>Status:</b> 40204 &nbsp;&nbsp;<b>Description:</b> The email account input is invalid. Please check and try again."},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "LeadFinderController_aiChat": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/leads/ai-chat";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["conversationId"] !== undefined) setBodyField(body as IDataObject, {"name":"conversationId","displayName":"Conversation Id","description":"Existing conversation id to continue a previous AI chat session","type":"string"}, additionalFields["conversationId"], this, itemIndex);
    if (additionalFields["limit"] !== undefined) setBodyField(body as IDataObject, {"name":"limit","displayName":"Limit","description":"Maximum number of results to return (1-200). Defaults to 25. Larger values are fetched by paginating the underlying search.","type":"number","minValue":1,"maxValue":200,"default":25}, additionalFields["limit"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"query","displayName":"Query","description":"Natural-language query describing the leads or companies to find","type":"string","required":true,"example":"Find fintech companies with 51-200 employees"}, this.getNodeParameter("query", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"4000: Failed to process AI chat request<br><br>"}};
        break;
      }
    case "LeadFinderController_bulkAddLeadsToSequence": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/leads/bulk-actions/add-to-sequence";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"leadIds","displayName":"Lead Ids","description":"Lead IDs to add to the sequence (max 10000)","type":"array","required":true,"example":[12345,67890],"representation":"raw","items":{"name":"item","displayName":"Item","type":"number"}}, this.getNodeParameter("leadIds", itemIndex), this, itemIndex);
    if (additionalFields["newTags"] !== undefined) setBodyField(body as IDataObject, {"name":"newTags","displayName":"New Tags","description":"New tag names to create and assign to the leads","type":"array","example":["Tag1","Tag2"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["newTags"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"sequenceId","displayName":"Sequence Id","description":"Sequence ID","type":"string","required":true,"example":"bwOLEx4l8G"}, this.getNodeParameter("sequenceId", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"stepId","displayName":"Step Id","description":"Step ID of the sequence","type":"string","required":true,"example":"2dP27N0gZ4"}, this.getNodeParameter("stepId", itemIndex), this, itemIndex);
    if (additionalFields["tagIds"] !== undefined) setBodyField(body as IDataObject, {"name":"tagIds","displayName":"Tag Ids","description":"Existing tag IDs to assign to the leads","type":"array","example":["VMw56r9jPb"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["tagIds"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"4000: Failed to add leads to sequence<br><br>"},"401":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "LeadFinderController_enrichCompanies": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/enrich/company";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["company_domain"] !== undefined) setBodyField(body as IDataObject, {"name":"company_domain","displayName":"Company domain","description":"Company domains to enrich (max 100)","type":"array","example":["acme.com"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["company_domain"], this, itemIndex);
    if (additionalFields["company_id"] !== undefined) setBodyField(body as IDataObject, {"name":"company_id","displayName":"Company id","description":"Company IDs to enrich (max 100)","type":"array","example":[12345],"representation":"raw","items":{"name":"item","displayName":"Item","type":"number"}}, additionalFields["company_id"], this, itemIndex);
    if (additionalFields["company_website"] !== undefined) setBodyField(body as IDataObject, {"name":"company_website","displayName":"Company website","description":"Company website URLs to enrich (max 100)","type":"array","example":["https://acme.com"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["company_website"], this, itemIndex);
    if (additionalFields["linkedin_url"] !== undefined) setBodyField(body as IDataObject, {"name":"linkedin_url","displayName":"Linkedin url","description":"LinkedIn company URLs to enrich (max 100)","type":"array","example":["https://linkedin.com/company/acme"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["linkedin_url"], this, itemIndex);
    if (additionalFields["webhook_url"] !== undefined) setBodyField(body as IDataObject, {"name":"webhook_url","displayName":"Webhook url","description":"Webhook URL to call when enrichment completes","type":"string"}, additionalFields["webhook_url"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"4000: Failed to enrich companies<br><br>"}};
        break;
      }
    case "LeadFinderController_enrichLeads": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/enrich/contact";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["email_type"] !== undefined) setBodyField(body as IDataObject, {"name":"email_type","displayName":"Email type","description":"Email type to reveal. With reveal_phone=false, returns the selected email; with true, work_email returns work email and phone, while personal_email returns both email types and phone. Not allowed when profile_only is true.","type":"string","enum":["work_email","personal_email"],"default":"work_email"}, additionalFields["email_type"], this, itemIndex);
    if (additionalFields["full_name_with_company"] !== undefined) setBodyField(body as IDataObject, {"name":"full_name_with_company","displayName":"Full name with company","description":"Full name + company combinations to enrich (max 100)","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"company_domain","displayName":"Company domain","description":"Company domain (required if company_website is absent or empty)","type":"string","example":"acme.com"},{"name":"company_website","displayName":"Company website","description":"Company website URL (required if company_domain is absent or empty)","type":"string","example":"https://acme.com"},{"name":"first_name","displayName":"First name","description":"First name of the person","type":"string"},{"name":"last_name","displayName":"Last name","description":"Last name of the person","type":"string"}]}}, additionalFields["full_name_with_company"], this, itemIndex);
    if (additionalFields["lead_id"] !== undefined) setBodyField(body as IDataObject, {"name":"lead_id","displayName":"Lead id","description":"Lead IDs to enrich (max 100)","type":"array","example":[12345,67890],"representation":"raw","items":{"name":"item","displayName":"Item","type":"number"}}, additionalFields["lead_id"], this, itemIndex);
    if (additionalFields["linkedin_url"] !== undefined) setBodyField(body as IDataObject, {"name":"linkedin_url","displayName":"Linkedin url","description":"LinkedIn profile URLs to enrich (max 100)","type":"array","example":["https://linkedin.com/in/johndoe"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["linkedin_url"], this, itemIndex);
    if (additionalFields["profile_only"] !== undefined) setBodyField(body as IDataObject, {"name":"profile_only","displayName":"Profile only","description":"Save profile data only (name, job title, company, location, LinkedIn) with no contact lookup, at a reduced credit cost. Mutually exclusive with `email_type` and `reveal_phone: true` — sending either alongside it is rejected.","type":"boolean","default":false}, additionalFields["profile_only"], this, itemIndex);
    if (additionalFields["reveal_phone"] !== undefined) setBodyField(body as IDataObject, {"name":"reveal_phone","displayName":"Reveal phone","description":"Whether to reveal phone numbers","type":"boolean","default":false}, additionalFields["reveal_phone"], this, itemIndex);
    if (additionalFields["webhook_url"] !== undefined) setBodyField(body as IDataObject, {"name":"webhook_url","displayName":"Webhook url","description":"Webhook URL to call when enrichment completes","type":"string"}, additionalFields["webhook_url"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"4000: Failed to enrich leads<br><br>1001: profile_only cannot be combined with email_type or reveal_phone<br><br>"}};
        break;
      }
    case "LeadFinderController_getCreditDetails": {
        
        
        const path = "/v1/credits";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "LeadFinderController_getEnrichmentResult": {
        
        
        let path = "/v1/enrich/status/result/{requestId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{requestId}").join(encodeURIComponent(String(this.getNodeParameter("requestId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"404":{"title":"40000: Enrichment job not found<br><br>"}};
        break;
      }
    case "LeadFinderController_getEnrichmentStatus": {
        
        
        let path = "/v1/enrich/status/{requestId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{requestId}").join(encodeURIComponent(String(this.getNodeParameter("requestId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"404":{"title":"40000: Enrichment job not found<br><br>"}};
        break;
      }
    case "LeadFinderController_getLeadFinderFilters": {
        
        
        const path = "/v1/search/filters";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "LeadFinderController_getRateLimitStatus": {
        
        
        const path = "/v1/enrich/rate-limits";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "LeadFinderController_searchCompanies": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/search/companies";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["active_job_postings_count"] !== undefined) setBodyField(body as IDataObject, {"name":"active_job_postings_count","displayName":"Active job postings count","type":"object","example":{"max":"50","min":"5"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["active_job_postings_count"], this, itemIndex);
    if (additionalFields["active_job_postings_title"] !== undefined) setBodyField(body as IDataObject, {"name":"active_job_postings_title","displayName":"Active job postings title","type":"object","example":{"includes":["Software Engineer","Product Manager"]},"representation":"raw","fields":[{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["active_job_postings_title"], this, itemIndex);
    if (additionalFields["base_salary"] !== undefined) setBodyField(body as IDataObject, {"name":"base_salary","displayName":"Base salary","type":"array","example":[{"range":{"max":"50000","min":"30000"},"title":{"exactMatch":0,"excludes":[],"includes":["utility sales and service manager"]}}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"range","displayName":"Range","type":"object","representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]},{"name":"title","displayName":"Title","type":"object","representation":"raw","fields":[{"name":"exactMatch","displayName":"Exact Match","type":"number","enum":[0,1]},{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}]}}, additionalFields["base_salary"], this, itemIndex);
    if (additionalFields["company_annual_revenue"] !== undefined) setBodyField(body as IDataObject, {"name":"company_annual_revenue","displayName":"Company annual revenue","type":"object","example":{"max":"50000000","min":"1000000"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["company_annual_revenue"], this, itemIndex);
    if (additionalFields["company_domain"] !== undefined) setBodyField(body as IDataObject, {"name":"company_domain","displayName":"Company domain","type":"object","example":{"excludes":["meta.com"],"includes":["saleshandy.com","google.com"]},"representation":"raw","fields":[{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["company_domain"], this, itemIndex);
    if (additionalFields["company_founded_year"] !== undefined) setBodyField(body as IDataObject, {"name":"company_founded_year","displayName":"Company founded year","type":"object","example":{"max":"2026","min":"2021"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["company_founded_year"], this, itemIndex);
    if (additionalFields["company_funding_amount"] !== undefined) setBodyField(body as IDataObject, {"name":"company_funding_amount","displayName":"Company funding amount","type":"object","example":{"max":"100000000","min":"5000000"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["company_funding_amount"], this, itemIndex);
    if (additionalFields["company_funding_date"] !== undefined) setBodyField(body as IDataObject, {"name":"company_funding_date","displayName":"Company funding date","type":"object","example":{"max":"2026-03-23","min":"2026-02-21"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["company_funding_date"], this, itemIndex);
    if (additionalFields["company_hq_location"] !== undefined) setBodyField(body as IDataObject, {"name":"company_hq_location","displayName":"Company hq location","type":"object","example":{"exactMatch":0,"excludes":["California, USA"],"includes":["Gujarat, India"]},"representation":"raw","fields":[{"name":"exactMatch","displayName":"Exact Match","type":"number","enum":[0,1]},{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["company_hq_location"], this, itemIndex);
    if (additionalFields["company_industry"] !== undefined) setBodyField(body as IDataObject, {"name":"company_industry","displayName":"Company industry","type":"object","example":{"excludes":["Retail"],"includes":["Software Development"]},"representation":"raw","fields":[{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["company_industry"], this, itemIndex);
    if (additionalFields["company_name"] !== undefined) setBodyField(body as IDataObject, {"name":"company_name","displayName":"Company name","type":"object","example":{"exactMatch":0,"excludes":["Meta"],"includes":["Google","Microsoft"]},"representation":"raw","fields":[{"name":"exactMatch","displayName":"Exact Match","type":"number","enum":[0,1],"default":0},{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["company_name"], this, itemIndex);
    if (additionalFields["company_size"] !== undefined) setBodyField(body as IDataObject, {"name":"company_size","displayName":"Company size","type":"object","example":{"max":"500","min":"50"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["company_size"], this, itemIndex);
    if (additionalFields["employee_count_department"] !== undefined) setBodyField(body as IDataObject, {"name":"employee_count_department","displayName":"Employee count department","type":"array","example":[{"count":{"max":"50","min":"5"},"group":"medical"},{"count":{"max":"100","min":"10"},"group":"sales"}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"count","displayName":"Count","type":"object","representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]},{"name":"group","displayName":"Group","type":"string","enum":["medical","sales","hr","legal","marketing","finance","technical","consulting","operations","product","general_management","administrative","customer_service","project_management","design","research","trades","real_estate","education","other_department"]}]}}, additionalFields["employee_count_department"], this, itemIndex);
    if (additionalFields["employee_count_seniority"] !== undefined) setBodyField(body as IDataObject, {"name":"employee_count_seniority","displayName":"Employee count seniority","type":"array","example":[{"count":{"max":"5","min":"1"},"group":"owner"},{"count":{"max":"15","min":"3"},"group":"vp"}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"count","displayName":"Count","type":"object","representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]},{"name":"group","displayName":"Group","type":"string","enum":["owner","founder","clevel","partner","vp","head","director","manager","senior","mid","junior","intern","specialist","other_management"]}]}}, additionalFields["employee_count_seniority"], this, itemIndex);
    if (additionalFields["employee_reviews_aggregate_score"] !== undefined) setBodyField(body as IDataObject, {"name":"employee_reviews_aggregate_score","displayName":"Employee reviews aggregate score","type":"object","example":{"max":"2","min":"1"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["employee_reviews_aggregate_score"], this, itemIndex);
    if (additionalFields["employee_reviews_business_outlook"] !== undefined) setBodyField(body as IDataObject, {"name":"employee_reviews_business_outlook","displayName":"Employee reviews business outlook","type":"object","example":{"max":"3","min":"2"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["employee_reviews_business_outlook"], this, itemIndex);
    if (additionalFields["employee_reviews_career_opportunities"] !== undefined) setBodyField(body as IDataObject, {"name":"employee_reviews_career_opportunities","displayName":"Employee reviews career opportunities","type":"object","example":{"max":"2","min":"1"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["employee_reviews_career_opportunities"], this, itemIndex);
    if (additionalFields["employee_reviews_ceo_approval"] !== undefined) setBodyField(body as IDataObject, {"name":"employee_reviews_ceo_approval","displayName":"Employee reviews ceo approval","type":"object","example":{"max":"3","min":"2"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["employee_reviews_ceo_approval"], this, itemIndex);
    if (additionalFields["employee_reviews_culture_values"] !== undefined) setBodyField(body as IDataObject, {"name":"employee_reviews_culture_values","displayName":"Employee reviews culture values","type":"object","example":{"max":"2","min":"1"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["employee_reviews_culture_values"], this, itemIndex);
    if (additionalFields["employee_reviews_recommend"] !== undefined) setBodyField(body as IDataObject, {"name":"employee_reviews_recommend","displayName":"Employee reviews recommend","type":"object","example":{"max":"2","min":"1"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["employee_reviews_recommend"], this, itemIndex);
    if (additionalFields["employee_reviews_total_count"] !== undefined) setBodyField(body as IDataObject, {"name":"employee_reviews_total_count","displayName":"Employee reviews total count","type":"object","example":{"max":"10","min":"1"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["employee_reviews_total_count"], this, itemIndex);
    if (additionalFields["employee_reviews_work_life_balance"] !== undefined) setBodyField(body as IDataObject, {"name":"employee_reviews_work_life_balance","displayName":"Employee reviews work life balance","type":"object","example":{"max":"2","min":"1"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["employee_reviews_work_life_balance"], this, itemIndex);
    if (additionalFields["followers_count_linkedin"] !== undefined) setBodyField(body as IDataObject, {"name":"followers_count_linkedin","displayName":"Followers count linkedin","type":"object","example":{"max":"100000","min":"5000"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["followers_count_linkedin"], this, itemIndex);
    if (additionalFields["funding_rounds_name"] !== undefined) setBodyField(body as IDataObject, {"name":"funding_rounds_name","displayName":"Funding rounds name","type":"object","example":{"includes":["Series rounds","Seed"]},"representation":"raw","fields":[{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["funding_rounds_name"], this, itemIndex);
    if (additionalFields["is_b2b"] !== undefined) setBodyField(body as IDataObject, {"name":"is_b2b","displayName":"Is b2b","type":"boolean","example":true}, additionalFields["is_b2b"], this, itemIndex);
    if (additionalFields["keywords"] !== undefined) setBodyField(body as IDataObject, {"name":"keywords","displayName":"Keywords","type":"object","example":{"includes":["artificial intelligence","machine learning"]},"representation":"raw","fields":[{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["keywords"], this, itemIndex);
    if (additionalFields["last_funding_round_amount_raised"] !== undefined) setBodyField(body as IDataObject, {"name":"last_funding_round_amount_raised","displayName":"Last funding round amount raised","type":"object","example":{"max":"20000000","min":"1000000"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["last_funding_round_amount_raised"], this, itemIndex);
    if (additionalFields["last_funding_round_announced_date"] !== undefined) setBodyField(body as IDataObject, {"name":"last_funding_round_announced_date","displayName":"Last funding round announced date","type":"object","example":{"max":"2025-12-31","min":"2024-06-01"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["last_funding_round_announced_date"], this, itemIndex);
    if (additionalFields["last_funding_round_name"] !== undefined) setBodyField(body as IDataObject, {"name":"last_funding_round_name","displayName":"Last funding round name","type":"object","example":{"includes":["Series rounds","Private Equity"]},"representation":"raw","fields":[{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["last_funding_round_name"], this, itemIndex);
    if (additionalFields["linkedin_url"] !== undefined) setBodyField(body as IDataObject, {"name":"linkedin_url","displayName":"Linkedin url","type":"object","example":{"exactMatch":0,"excludes":["https://linkedin.com/company/meta"],"includes":["https://linkedin.com/company/hubspot"]},"representation":"raw","fields":[{"name":"exactMatch","displayName":"Exact Match","type":"number","enum":[0,1]},{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["linkedin_url"], this, itemIndex);
    if (additionalFields["look_alike_company_name"] !== undefined) setBodyField(body as IDataObject, {"name":"look_alike_company_name","displayName":"Look alike company name","type":"array","example":[{"value":"hubspot.com"},{"value":"Salesforce"}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"_annual_revenue","displayName":"Annual revenue","type":"number"},{"name":"_categories_and_keywords","displayName":"Categories and keywords","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"_company_name","displayName":"Company name","type":"string"},{"name":"_competitors","displayName":"Competitors","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"_followers_count_linkedin","displayName":"Followers count linkedin","type":"number"},{"name":"_industry","displayName":"Industry","type":"string"},{"name":"_ownership_status","displayName":"Ownership status","type":"string"},{"name":"_technologies_used","displayName":"Technologies used","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"_total_website_visits_monthly","displayName":"Total website visits monthly","type":"number"},{"name":"company_id","displayName":"Company id","type":"number"},{"name":"value","displayName":"Value","type":"string"}]}}, additionalFields["look_alike_company_name"], this, itemIndex);
    if (additionalFields["naics_codes"] !== undefined) setBodyField(body as IDataObject, {"name":"naics_codes","displayName":"Naics codes","type":"object","example":{"includes":["511210"]},"representation":"raw","fields":[{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["naics_codes"], this, itemIndex);
    if (additionalFields["newsIds"] !== undefined) setBodyField(body as IDataObject, {"name":"newsIds","displayName":"News Ids","type":"array","example":[2,4,7],"representation":"raw","items":{"name":"item","displayName":"Item","type":"number"}}, additionalFields["newsIds"], this, itemIndex);
    if (additionalFields["ownership_status"] !== undefined) setBodyField(body as IDataObject, {"name":"ownership_status","displayName":"Ownership status","type":"string","enum":["Private","Public","Other"],"example":"Private"}, additionalFields["ownership_status"], this, itemIndex);
    if (additionalFields["page"] !== undefined) setBodyField(body as IDataObject, {"name":"page","displayName":"Page","type":"number","minValue":1,"maxValue":400,"default":1,"example":1}, additionalFields["page"], this, itemIndex);
    if (additionalFields["product_reviews_aggregate_score"] !== undefined) setBodyField(body as IDataObject, {"name":"product_reviews_aggregate_score","displayName":"Product reviews aggregate score","type":"object","example":{"max":"5.0","min":"3.5"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["product_reviews_aggregate_score"], this, itemIndex);
    if (additionalFields["product_reviews_count"] !== undefined) setBodyField(body as IDataObject, {"name":"product_reviews_count","displayName":"Product reviews count","type":"object","example":{"max":"10","min":"1"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["product_reviews_count"], this, itemIndex);
    if (additionalFields["product_reviews_score_change"] !== undefined) setBodyField(body as IDataObject, {"name":"product_reviews_score_change","displayName":"Product reviews score change","type":"object","example":{"direction":"down","duration":"monthly","points_range":{"max":"4.5","min":"2.0"}},"representation":"raw","fields":[{"name":"direction","displayName":"Direction","type":"string","enum":["up","down","any"]},{"name":"duration","displayName":"Duration","type":"string","enum":["current","monthly","quarterly","yearly"]},{"name":"points_range","displayName":"Points range","type":"object","representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}]}, additionalFields["product_reviews_score_change"], this, itemIndex);
    if (additionalFields["rank_global"] !== undefined) setBodyField(body as IDataObject, {"name":"rank_global","displayName":"Rank global","type":"object","example":{"max":"100000","min":"1"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["rank_global"], this, itemIndex);
    if (additionalFields["sic_codes"] !== undefined) setBodyField(body as IDataObject, {"name":"sic_codes","displayName":"Sic codes","type":"object","example":{"includes":["7372"]},"representation":"raw","fields":[{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["sic_codes"], this, itemIndex);
    if (additionalFields["signalIds"] !== undefined) setBodyField(body as IDataObject, {"name":"signalIds","displayName":"Signal Ids","type":"array","example":[2,3,4],"representation":"raw","items":{"name":"item","displayName":"Item","type":"number","minValue":1,"maxValue":10}}, additionalFields["signalIds"], this, itemIndex);
    if (additionalFields["social_urls"] !== undefined) setBodyField(body as IDataObject, {"name":"social_urls","displayName":"Social urls","type":"object","example":{"includes":["https://twitter.com/HubSpot"]},"representation":"raw","fields":[{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["social_urls"], this, itemIndex);
    if (additionalFields["technologies_used"] !== undefined) setBodyField(body as IDataObject, {"name":"technologies_used","displayName":"Technologies used","type":"object","example":{"includes":["React","AWS","Salesforce CRM"]},"representation":"raw","fields":[{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["technologies_used"], this, itemIndex);
    if (additionalFields["total_website_visits_monthly"] !== undefined) setBodyField(body as IDataObject, {"name":"total_website_visits_monthly","displayName":"Total website visits monthly","type":"object","example":{"max":"500000","min":"10000"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["total_website_visits_monthly"], this, itemIndex);
    if (additionalFields["type"] !== undefined) setBodyField(body as IDataObject, {"name":"type","displayName":"Type","type":"string","enum":["Privately Held","Public Company","Self-Owned","Self-Employed","Partnership","Nonprofit","Educational","Government Agency"],"example":"Privately Held"}, additionalFields["type"], this, itemIndex);
    if (additionalFields["visits_breakdown_by_country"] !== undefined) setBodyField(body as IDataObject, {"name":"visits_breakdown_by_country","displayName":"Visits breakdown by country","type":"array","example":[{"country":"United States","percentage":{"max":"60","min":"20"}}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"country","displayName":"Country","type":"string"},{"name":"percentage","displayName":"Percentage","type":"object","representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}]}}, additionalFields["visits_breakdown_by_country"], this, itemIndex);
    if (additionalFields["visits_breakdown_by_gender"] !== undefined) setBodyField(body as IDataObject, {"name":"visits_breakdown_by_gender","displayName":"Visits breakdown by gender","type":"array","example":[{"gender":"female","percentage":{"max":"70","min":"30"}}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"gender","displayName":"Gender","type":"string","enum":["male","female"]},{"name":"percentage","displayName":"Percentage","type":"object","representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}]}}, additionalFields["visits_breakdown_by_gender"], this, itemIndex);
    if (additionalFields["website"] !== undefined) setBodyField(body as IDataObject, {"name":"website","displayName":"Website","type":"object","example":{"includes":["https://www.hubspot.com"]},"representation":"raw","fields":[{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["website"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "LeadFinderController_searchLeads": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/search/people";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["certifications"] !== undefined) setBodyField(body as IDataObject, {"name":"certifications","displayName":"Certifications","type":"object","example":{"includes":["PMP","AWS Solutions Architect"]},"representation":"raw","fields":[{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["certifications"], this, itemIndex);
    if (additionalFields["company_annual_revenue"] !== undefined) setBodyField(body as IDataObject, {"name":"company_annual_revenue","displayName":"Company annual revenue","type":"object","example":{"max":"50000000","min":"1000000"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["company_annual_revenue"], this, itemIndex);
    if (additionalFields["company_bootstraped"] !== undefined) setBodyField(body as IDataObject, {"name":"company_bootstraped","displayName":"Company bootstraped","type":"boolean","example":true}, additionalFields["company_bootstraped"], this, itemIndex);
    if (additionalFields["company_domain"] !== undefined) setBodyField(body as IDataObject, {"name":"company_domain","displayName":"Company domain","type":"object","example":{"excludes":["google.com"],"includes":["hubspot.com","salesforce.com"]},"representation":"raw","fields":[{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["company_domain"], this, itemIndex);
    if (additionalFields["company_founded_year"] !== undefined) setBodyField(body as IDataObject, {"name":"company_founded_year","displayName":"Company founded year","type":"object","example":{"max":"2023","min":"2015"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["company_founded_year"], this, itemIndex);
    if (additionalFields["company_funding_amount"] !== undefined) setBodyField(body as IDataObject, {"name":"company_funding_amount","displayName":"Company funding amount","type":"object","example":{"max":"100000000","min":"5000000"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["company_funding_amount"], this, itemIndex);
    if (additionalFields["company_funding_date"] !== undefined) setBodyField(body as IDataObject, {"name":"company_funding_date","displayName":"Company funding date","type":"object","example":{"max":"2025-12-31","min":"2024-01-01"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["company_funding_date"], this, itemIndex);
    if (additionalFields["company_hq_location"] !== undefined) setBodyField(body as IDataObject, {"name":"company_hq_location","displayName":"Company hq location","type":"object","example":{"exactMatch":0,"excludes":["China"],"includes":["United States","United Kingdom"]},"representation":"raw","fields":[{"name":"exactMatch","displayName":"Exact Match","type":"number","enum":[0,1]},{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["company_hq_location"], this, itemIndex);
    if (additionalFields["company_industry"] !== undefined) setBodyField(body as IDataObject, {"name":"company_industry","displayName":"Company industry","type":"object","example":{"excludes":["IT Services and IT Consulting"],"includes":["Software Development","Information Technology & Services"]},"representation":"raw","fields":[{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["company_industry"], this, itemIndex);
    if (additionalFields["company_name"] !== undefined) setBodyField(body as IDataObject, {"name":"company_name","displayName":"Company name","type":"object","example":{"exactMatch":0,"excludes":["Google"],"includes":["HubSpot","Salesforce"],"shouldSearchPast":0},"representation":"raw","fields":[{"name":"exactMatch","displayName":"Exact Match","type":"number","enum":[0,1],"default":0},{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"shouldSearchPast","displayName":"Should Search Past","type":"number"}]}, additionalFields["company_name"], this, itemIndex);
    if (additionalFields["company_size"] !== undefined) setBodyField(body as IDataObject, {"name":"company_size","displayName":"Company size","type":"object","example":{"max":"500","min":"50"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["company_size"], this, itemIndex);
    if (additionalFields["current_company_experience"] !== undefined) setBodyField(body as IDataObject, {"name":"current_company_experience","displayName":"Current company experience","type":"object","example":{"max":"5","min":"1"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["current_company_experience"], this, itemIndex);
    if (additionalFields["current_role_tenure"] !== undefined) setBodyField(body as IDataObject, {"name":"current_role_tenure","displayName":"Current role tenure","type":"object","example":{"max":"2","min":"0"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["current_role_tenure"], this, itemIndex);
    if (additionalFields["decision_maker"] !== undefined) setBodyField(body as IDataObject, {"name":"decision_maker","displayName":"Decision maker","type":"boolean","example":true}, additionalFields["decision_maker"], this, itemIndex);
    if (additionalFields["department"] !== undefined) setBodyField(body as IDataObject, {"name":"department","displayName":"Department","type":"object","example":{"excludes":["Customer Service"],"includes":["Sales","Marketing"]},"representation":"raw","fields":[{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["department"], this, itemIndex);
    if (additionalFields["education_degrees"] !== undefined) setBodyField(body as IDataObject, {"name":"education_degrees","displayName":"Education degrees","type":"object","example":{"excludes":["Diploma"],"includes":["MBA"]},"representation":"raw","fields":[{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["education_degrees"], this, itemIndex);
    if (additionalFields["full_name"] !== undefined) setBodyField(body as IDataObject, {"name":"full_name","displayName":"Full name","type":"object","example":{"exactMatch":0,"includes":["Jane Doe"]},"representation":"raw","fields":[{"name":"exactMatch","displayName":"Exact Match","type":"number","enum":[0,1]},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["full_name"], this, itemIndex);
    if (additionalFields["is_b2b"] !== undefined) setBodyField(body as IDataObject, {"name":"is_b2b","displayName":"Is b2b","type":"boolean","example":true}, additionalFields["is_b2b"], this, itemIndex);
    if (additionalFields["job_title"] !== undefined) setBodyField(body as IDataObject, {"name":"job_title","displayName":"Job title","type":"object","example":{"exactMatch":0,"excludes":["Assistant","Intern"],"includes":["VP of Sales","Head of Sales"],"shouldSearchPast":0},"representation":"raw","fields":[{"name":"exactMatch","displayName":"Exact Match","type":"number","enum":[0,1]},{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"shouldSearchPast","displayName":"Should Search Past","type":"number"}]}, additionalFields["job_title"], this, itemIndex);
    if (additionalFields["jobs_changed_within"] !== undefined) setBodyField(body as IDataObject, {"name":"jobs_changed_within","displayName":"Jobs changed within","type":"object","example":{"max":"2","min":"0"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["jobs_changed_within"], this, itemIndex);
    if (additionalFields["keywords"] !== undefined) setBodyField(body as IDataObject, {"name":"keywords","displayName":"Keywords","type":"object","example":{"includes":["artificial intelligence","growth marketing"]},"representation":"raw","fields":[{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["keywords"], this, itemIndex);
    if (additionalFields["limitPerCompany"] !== undefined) setBodyField(body as IDataObject, {"name":"limitPerCompany","displayName":"Limit Per Company","type":"number","minValue":1,"maxValue":10000,"example":5}, additionalFields["limitPerCompany"], this, itemIndex);
    if (additionalFields["linkedin_connection_count"] !== undefined) setBodyField(body as IDataObject, {"name":"linkedin_connection_count","displayName":"Linkedin connection count","type":"object","example":{"max":"5000","min":"500"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["linkedin_connection_count"], this, itemIndex);
    if (additionalFields["linkedin_followers_count"] !== undefined) setBodyField(body as IDataObject, {"name":"linkedin_followers_count","displayName":"Linkedin followers count","type":"object","example":{"max":"10000","min":"500"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["linkedin_followers_count"], this, itemIndex);
    if (additionalFields["linkedin_url"] !== undefined) setBodyField(body as IDataObject, {"name":"linkedin_url","displayName":"Linkedin url","type":"array","example":["https://linkedin.com/in/example-profile"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["linkedin_url"], this, itemIndex);
    if (additionalFields["location"] !== undefined) setBodyField(body as IDataObject, {"name":"location","displayName":"Location","type":"object","example":{"exactMatch":0,"excludes":["Los Angeles, California, United States"],"includes":["California, United States","New York, United States"]},"representation":"raw","fields":[{"name":"exactMatch","displayName":"Exact Match","type":"number","enum":[0,1]},{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["location"], this, itemIndex);
    if (additionalFields["management_level"] !== undefined) setBodyField(body as IDataObject, {"name":"management_level","displayName":"Management level","type":"object","example":{"excludes":["Intern"],"includes":["C-Level","Director","Manager"]},"representation":"raw","fields":[{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["management_level"], this, itemIndex);
    if (additionalFields["page"] !== undefined) setBodyField(body as IDataObject, {"name":"page","displayName":"Page","type":"number","minValue":1,"maxValue":400,"default":1,"example":1}, additionalFields["page"], this, itemIndex);
    if (additionalFields["projected_base_salary"] !== undefined) setBodyField(body as IDataObject, {"name":"projected_base_salary","displayName":"Projected base salary","type":"object","example":{"max":"300000","min":"100000"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["projected_base_salary"], this, itemIndex);
    if (additionalFields["skills"] !== undefined) setBodyField(body as IDataObject, {"name":"skills","displayName":"Skills","type":"object","example":{"exactMatch":0,"excludes":["Excel"],"includes":["Python","Machine Learning"]},"representation":"raw","fields":[{"name":"exactMatch","displayName":"Exact Match","type":"number","enum":[0,1]},{"name":"excludes","displayName":"Excludes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["skills"], this, itemIndex);
    if (additionalFields["social_link"] !== undefined) setBodyField(body as IDataObject, {"name":"social_link","displayName":"Social link","type":"object","example":{"includes":["https://twitter.com/example","https://github.com/example"]},"representation":"raw","fields":[{"name":"includes","displayName":"Includes","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}]}, additionalFields["social_link"], this, itemIndex);
    if (additionalFields["total_experience"] !== undefined) setBodyField(body as IDataObject, {"name":"total_experience","displayName":"Total experience","type":"object","example":{"max":"15","min":"5"},"representation":"raw","fields":[{"name":"max","displayName":"Max","type":"string"},{"name":"min","displayName":"Min","type":"string"}]}, additionalFields["total_experience"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "FieldController_createField": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/fields";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["fallbackText"] !== undefined) setBodyField(body as IDataObject, {"name":"fallbackText","displayName":"Fallback Text","description":"A default placeholder text to display when the field has no value for a prospect.","type":"string","example":"Not provided"}, additionalFields["fallbackText"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"fieldType","displayName":"Field Type","description":"The data type of the custom field. Accepted values: text, number, date, long-text, dropdown, currency.","type":"string","required":true,"enum":["text","number","date","long-text","dropdown","currency"],"example":"text"}, this.getNodeParameter("fieldType", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"label","displayName":"Label","description":"The display name of the custom field. This label will appear in the prospect profile and throughout the application.","type":"string","required":true,"example":"LinkedIn Profile URL"}, this.getNodeParameter("label", itemIndex), this, itemIndex);
    if (additionalFields["metadata"] !== undefined) setBodyField(body as IDataObject, {"name":"metadata","displayName":"Metadata","description":"Type-specific settings. Dropdown fields require metadata.options with up to 50 values. Currency fields require metadata.currencyCode, an ISO 4217 code; supported codes include USD, EUR, GBP, JPY, CHF, CAD, AUD, CNY, INR, SGD, HKD, NZD, KRW, SEK, AED, BRL, MXN, ZAR, and SAR. Ignored for other field types.","type":"object","example":{"options":["Free","Growth","Enterprise"]},"representation":"raw"}, additionalFields["metadata"], this, itemIndex);
    if (additionalFields["tags"] !== undefined) setBodyField(body as IDataObject, {"name":"tags","displayName":"Tags","description":"Freeform tags for finding this field later (separate namespace from Prospect Tags). Never blocks create — resolved case-insensitively, existing tags reused.","type":"array","example":["CRM","Sales"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["tags"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Default fields cannot be added as custom fields<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "FieldController_getFields": {
        
        
        const path = "/v1/fields";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        qs["systemFields"] = this.getNodeParameter("systemFields", itemIndex);
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to fetch the fields<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "FieldController_updateField": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/fields/{fieldId}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{fieldId}").join(encodeURIComponent(String(this.getNodeParameter("fieldId", itemIndex))));
        if (additionalFields["fallbackText"] !== undefined) setBodyField(body as IDataObject, {"name":"fallbackText","displayName":"Fallback Text","description":"Updated placeholder text to display when the field has no value for a prospect.","type":"string","example":"Not available"}, additionalFields["fallbackText"], this, itemIndex);
    if (additionalFields["label"] !== undefined) setBodyField(body as IDataObject, {"name":"label","displayName":"Label","description":"The new display name for the custom field. This label will appear in the prospect profile and throughout the application.","type":"string","example":"LinkedIn URL"}, additionalFields["label"], this, itemIndex);
    if (additionalFields["tags"] !== undefined) setBodyField(body as IDataObject, {"name":"tags","displayName":"Tags","description":"Full replacement list for this field?s tags, separate from prospect tags. Omit to keep current tags; pass [] to remove them. Supplied tags are added or reused case-insensitively, and omitted current tags are removed.","type":"array","example":["CRM","Sales"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["tags"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Field not found<br><br>1001: Default field names cannot be edited<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "LeadsToEmailController_createDomainsWorkflow": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/leads-to-email/domains";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"domains","displayName":"Domains","description":"Company domains to enrich. Each entry is a bare domain/URL string or an object { domain, jobTitle?, department?, managementLevel?, decisionMaker? } whose criteria filter the people picked for THAT domain.","type":"array","required":true,"example":["saleshandy.com",{"department":"Marketing","domain":"stripe.com","jobTitle":"VP"}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("domains", itemIndex), this, itemIndex);
    if (additionalFields["exportColumns"] !== undefined) setBodyField(body as IDataObject, {"name":"exportColumns","displayName":"Export Columns","description":"Columns for the enriched output. Defaults to the key-columns set when omitted or empty.","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["exportColumns"], this, itemIndex);
    if (additionalFields["filters"] !== undefined) setBodyField(body as IDataObject, {"name":"filters","displayName":"Filters","description":"Search filters AND-combined onto every domain's people search — e.g. { \"location\": { \"includes\": [\"United States\"] } }.","type":"object","representation":"raw"}, additionalFields["filters"], this, itemIndex);
    if (additionalFields["leadsPerDomain"] !== undefined) setBodyField(body as IDataObject, {"name":"leadsPerDomain","displayName":"Leads Per Domain","description":"How many people to enrich per domain (top-ranked by lead score). Defaults to 2 when omitted.","type":"number","minValue":1,"maxValue":10,"default":2}, additionalFields["leadsPerDomain"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"webhookUrl","displayName":"Webhook Url","description":"Webhook URL that receives the final result once enrichment completes (single webhook with the enriched records).","type":"string","required":true,"example":"https://example.com/webhooks/leads-to-email"}, this.getNodeParameter("webhookUrl", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"4000: Failed to create leads-to-email workflow<br><br>"},"401":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "LeadsToEmailController_createWorkflow": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/leads-to-email";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["bodyTemplateId"] !== undefined) setBodyField(body as IDataObject, {"name":"bodyTemplateId","displayName":"Body Template Id","type":"string"}, additionalFields["bodyTemplateId"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"columnMapping","displayName":"Column Mapping","type":"string","required":true,"example":"{\"First Name\":\"firstName\",\"Company Domain\":\"domain\"}"}, this.getNodeParameter("columnMapping", itemIndex), this, itemIndex);
    if (additionalFields["exportColumns"] !== undefined) setBodyField(body as IDataObject, {"name":"exportColumns","displayName":"Export Columns","type":"string","example":"[\"firstName\",\"lastName\",\"email\"]"}, additionalFields["exportColumns"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"file","displayName":"File","description":"CSV file with the leads to enrich","type":"string","format":"binary","required":true}, this.getNodeParameter("file", itemIndex), this, itemIndex);
    if (additionalFields["fileName"] !== undefined) setBodyField(body as IDataObject, {"name":"fileName","displayName":"File Name","type":"string","example":"q3-leads.csv"}, additionalFields["fileName"], this, itemIndex);
    if (additionalFields["filters"] !== undefined) setBodyField(body as IDataObject, {"name":"filters","displayName":"Filters","type":"string"}, additionalFields["filters"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"revealType","displayName":"Reveal Type","type":"string","required":true,"example":"email"}, this.getNodeParameter("revealType", itemIndex), this, itemIndex);
    if (additionalFields["stepContent"] !== undefined) setBodyField(body as IDataObject, {"name":"stepContent","displayName":"Step Content","description":"Stringified JSON array of caller-provided steps; skips AI compose","type":"string","example":"[{\"stepNumber\":1,\"subject\":\"Hi {{First Name}}\",\"content\":\"<p>Intro…</p>\"}]"}, additionalFields["stepContent"], this, itemIndex);
    if (additionalFields["stepGap"] !== undefined) setBodyField(body as IDataObject, {"name":"stepGap","displayName":"Step Gap","type":"string","example":"3"}, additionalFields["stepGap"], this, itemIndex);
    if (additionalFields["steps"] !== undefined) setBodyField(body as IDataObject, {"name":"steps","displayName":"Steps","type":"string","example":"3"}, additionalFields["steps"], this, itemIndex);
    if (additionalFields["subjectTemplateId"] !== undefined) setBodyField(body as IDataObject, {"name":"subjectTemplateId","displayName":"Subject Template Id","type":"string"}, additionalFields["subjectTemplateId"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"totalRecords","displayName":"Total Records","type":"string","required":true,"example":"250"}, this.getNodeParameter("totalRecords", itemIndex), this, itemIndex);
    if (additionalFields["webhookUrl"] !== undefined) setBodyField(body as IDataObject, {"name":"webhookUrl","displayName":"Webhook Url","type":"string","example":"https://example.com/webhooks/leads-to-email"}, additionalFields["webhookUrl"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: toFormData(body), json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"4000: Failed to create leads-to-email workflow<br><br>"},"401":{"title":"1001: Invalid token<br><br>"},"413":{"title":"CSV file exceeds the 50 MB upload limit"}};
        break;
      }
    case "NoteController_createNote": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/notes";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["attachmentIds"] !== undefined) setBodyField(body as IDataObject, {"name":"attachmentIds","displayName":"Attachment Ids","description":"IDs of attachments to associate with this note. Upload via POST /v1/notes/attachments to obtain each attachmentId.","type":"array","example":["Aw83gqRXvk","BV4yLpC2nX"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["attachmentIds"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"content","displayName":"Content","description":"The content of the note. Supports plain text and HTML.","type":"string","required":true,"example":"Follow up with this prospect next week regarding the demo."}, this.getNodeParameter("content", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"prospectIds","displayName":"Prospect Ids","description":"List of prospect IDs to attach the note to. Pass a single-element array for a single prospect, or multiple IDs to attach the same note to several prospects in one request.","type":"array","required":true,"example":["8PvBmrB7P7"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("prospectIds", itemIndex), this, itemIndex);
    if (additionalFields["status"] !== undefined) setBodyField(body as IDataObject, {"name":"status","displayName":"Status","description":"The status of the note. Use \"0\" for draft or \"1\" for published. Defaults to \"1\" (published) if not provided.","type":"string","enum":["0","1"],"default":"1","example":"1"}, additionalFields["status"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"visibility","displayName":"Visibility","description":"Who can see this note. Use \"1\" for public (visible to all team members) or \"2\" for private (visible only to you).","type":"string","required":true,"enum":["1","2"],"example":"1"}, this.getNodeParameter("visibility", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid prospect Id<br><br>1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "NoteController_updateNote": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/notes/{noteId}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{noteId}").join(encodeURIComponent(String(this.getNodeParameter("noteId", itemIndex))));
        if (additionalFields["content"] !== undefined) setBodyField(body as IDataObject, {"name":"content","displayName":"Content","description":"The updated content of the note. Supports plain text.","type":"string","example":"Prospect confirmed interest. Schedule a demo call for next Monday."}, additionalFields["content"], this, itemIndex);
    if (additionalFields["status"] !== undefined) setBodyField(body as IDataObject, {"name":"status","displayName":"Status","description":"Updated status of the note. Use \"0\" for draft or \"1\" for published.","type":"string","enum":["0","1"],"example":"1"}, additionalFields["status"], this, itemIndex);
    if (additionalFields["visibility"] !== undefined) setBodyField(body as IDataObject, {"name":"visibility","displayName":"Visibility","description":"Updated visibility setting. Use \"1\" for public (visible to all team members) or \"2\" for private (visible only to you).","type":"string","enum":["1","2"],"example":"2"}, additionalFields["visibility"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Note not found or access denied<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "NoteController_uploadNoteAttachment": {
        
        
        const path = "/v1/notes/attachments";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"file","displayName":"File","type":"string","format":"binary","required":true}, this.getNodeParameter("file", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: toFormData(body), json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4001: The file type you are trying to add is not supported.<br><br>4002: File exceeds the 20 MB upload limit. Please upload a smaller file.<br><br>4000: Failed to upload attachment<br><br>"}};
        break;
      }
    case "ProspectController_getProspectAttributeById": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/prospects/attribute";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        qs["attributeId"] = this.getNodeParameter("attributeId", itemIndex);
    if (additionalFields["prospectId"] !== undefined) qs["prospectId"] = additionalFields["prospectId"];
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"50001: Failed to fetch prospect attribute<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "ProspectController_getProspectImportStatus": {
        
        
        let path = "/v1/prospects/import-status/{requestId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{requestId}").join(encodeURIComponent(String(this.getNodeParameter("requestId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["isCompleted","reportURL"], simplified: ["isCompleted","reportURL"] };
        errorPlan = {"400":{"title":"40000: Prospect import request not found<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "ProspectController_getProspectNotes": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/prospects/{prospectId}/notes";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{prospectId}").join(encodeURIComponent(String(this.getNodeParameter("prospectId", itemIndex))));
    if (additionalFields["skip"] !== undefined) qs["skip"] = additionalFields["skip"];
    if (additionalFields["take"] !== undefined) qs["take"] = additionalFields["take"];
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "ProspectController_getProspectsVerificationStatus": {
        
        
        const path = "/v1/prospects/verification-status";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"emails","displayName":"Emails","type":"array","required":true,"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("emails", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["payload"], simplified: ["payload"] };
        errorPlan = {"400":{"title":"4000: Failed to fetch the prospect verification status<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "ProspectController_importProspects": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/prospects/import";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"conflictAction","displayName":"Conflict Action","description":"Action when a prospect already exists in the sequence: overwrite updates supplied fields but leaves blank cells unchanged; noUpdate skips existing prospects; addMissingFields fills only empty fields; upsert updates supplied fields and adds missing ones, with blank cells clearing existing values.","type":"string","required":true,"enum":["overwrite","noUpdate","addMissingFields","upsert"]}, this.getNodeParameter("conflictAction", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"prospectList","displayName":"Prospect List","description":"The array of prospect list. Each prospect is represented by an array of fields.","type":"object","required":true,"example":[{"fields":[{"id":"0VOLRwYe82","value":"john"},{"id":"pRjY60lBPa","value":"smith"},{"id":"KNz9Zgl7R3","value":"johnsmith@gmail.com"}]}],"representation":"raw"}, this.getNodeParameter("prospectList", itemIndex), this, itemIndex);
    if (additionalFields["stepId"] !== undefined) setBodyField(body as IDataObject, {"name":"stepId","displayName":"Step Id","description":"Optional. Set the step to which the prospect is to be added","type":"number","example":"0VOLRwYe82"}, additionalFields["stepId"], this, itemIndex);
    if (additionalFields["verifyProspects"] !== undefined) setBodyField(body as IDataObject, {"name":"verifyProspects","displayName":"Verify Prospects","description":"Optional. Set to \"true\" if prospect's email address should be verified","type":"boolean"}, additionalFields["verifyProspects"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to import prospects<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "ProspectController_importProspectsV2": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/prospects/import-with-field-name";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"conflictAction","displayName":"Conflict Action","description":"Action when a prospect already exists in the sequence: overwrite updates supplied fields but leaves blank cells unchanged; noUpdate skips existing prospects; addMissingFields fills only empty fields; upsert updates supplied fields and adds missing ones, with blank cells clearing existing values.","type":"string","required":true,"enum":["overwrite","noUpdate","addMissingFields","upsert"]}, this.getNodeParameter("conflictAction", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"prospectList","displayName":"Prospect List","description":"The array of prospect list. Each prospect is represented by an array of fields.","type":"array","required":true,"example":[{"Company":"ABC","Country":"USA","Email":"johnsmith@example.com","First Name":"Jon","Job Title":"Procurement Manager","Last Name":"Smith","Phone Number":"1234567890"},{"Company":"ABC","Country":"USA","Email":"jemmy@example.com","First Name":"Jemmy","Job Title":"Procurement Manager","Last Name":"Lorance","Phone Number":"1234567891"}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("prospectList", itemIndex), this, itemIndex);
    if (additionalFields["stepId"] !== undefined) setBodyField(body as IDataObject, {"name":"stepId","displayName":"Step Id","description":"Optional. Set the step to which the prospect is to be added","type":"string","example":"0VOLRwYe82"}, additionalFields["stepId"], this, itemIndex);
    if (additionalFields["tags"] !== undefined) setBodyField(body as IDataObject, {"name":"tags","displayName":"Tags","description":"Array of tags to be assigned to the imported prospects. If the tag does not exist, it will be created.","type":"array","example":["tag1","tag2","tag3"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["tags"], this, itemIndex);
    if (additionalFields["verifyProspects"] !== undefined) setBodyField(body as IDataObject, {"name":"verifyProspects","displayName":"Verify Prospects","description":"Optional. Set to \"true\" if prospect's email address should be verified","type":"boolean"}, additionalFields["verifyProspects"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to import prospects<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "ProspectController_upsertAttribute": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/prospects/{prospectId}/attribute";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{prospectId}").join(encodeURIComponent(String(this.getNodeParameter("prospectId", itemIndex))));
        if (additionalFields["attributeValue"] !== undefined) setBodyField(body as IDataObject, {"name":"attributeValue","displayName":"Attribute Value","description":"The new value to set for the field specified by \"fieldId\". Required when \"fieldId\" is provided.","type":"string","example":"Smith"}, additionalFields["attributeValue"], this, itemIndex);
    if (additionalFields["attributes"] !== undefined) setBodyField(body as IDataObject, {"name":"attributes","displayName":"Attributes","description":"A list of field updates to apply in a single request. Use this to update multiple fields at once. Each item must specify a fieldId and the new value. When this is provided, \"fieldId\" and \"attributeValue\" are ignored.","type":"array","example":[{"attributeValue":"John","fieldId":"BVaD1mKgzo"},{"attributeValue":"Smith","fieldId":"1qPB1GBBwD"}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"attributeValue","displayName":"Attribute Value","description":"The new value to set for this field.","type":"string","required":true,"example":"John"},{"name":"fieldId","displayName":"Field Id","description":"The ID of the field to update. Use the field IDs returned by the GET /fields endpoint.","type":"string","required":true,"example":"BVaD1mKgzo"}]}}, additionalFields["attributes"], this, itemIndex);
    if (additionalFields["fieldId"] !== undefined) setBodyField(body as IDataObject, {"name":"fieldId","displayName":"Field Id","description":"The ID of the field to update. Use this for updating a single field. Use the field IDs returned by the GET /fields endpoint. Required when \"attributes\" is not provided.","type":"string","example":"1qPB1GBBwD"}, additionalFields["fieldId"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"50001: Failed to prospect attribute<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceContactController_assignTagsToProspects": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/prospects/tags/assign";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["prospects"] !== undefined) setBodyField(body as IDataObject, {"name":"prospects","displayName":"Prospects","description":"IDs of the prospects","type":"array","example":["2dP27N0gZ4","bzZWZpl4wM","2dP27NrgZ3"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"number"}}, additionalFields["prospects"], this, itemIndex);
    if (additionalFields["prospectsEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"prospectsEmails","displayName":"Prospects Emails","description":"Email addresses associated with the prospects","type":"array","example":["example2@example.com","example1@example.com"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["prospectsEmails"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"tags","displayName":"Tags","description":"Names of the new tags to be created and attached to the given prospects","type":"array","required":true,"example":["Tag1","Tag2"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("tags", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Error codes: 40501 invalid prospects; 40502 invalid prospect emails; 40503 provide at least one of prospects or prospect emails."},"403":{"title":"4003: You don't have required permissions to perform this task<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceContactController_findProspects": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/prospects";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
    qs["pageSize"] = this.getNodeParameter("pageSize", itemIndex);
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    qs["sort"] = this.getNodeParameter("sort", itemIndex);
    qs["sortBy"] = this.getNodeParameter("sortBy", itemIndex);
    if (additionalFields["includeCustomFields"] !== undefined) qs["includeCustomFields"] = additionalFields["includeCustomFields"];
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to fetch prospects<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceContactController_findTags": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/prospects/tags";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to fetch prospect tags<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceContactController_getContactMinimalSequences": {
        
        
        let path = "/v1/prospects/{contactId}/minimal/sequences";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{contactId}").join(encodeURIComponent(String(this.getNodeParameter("contactId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to fetch contact sequence histories<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceContactController_unAssignTagsToProspects": {
        
        
        const path = "/v1/prospects/tags/un-assign";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        let body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        body = normalizeJsonValue(this.getNodeParameter("bodyJson", itemIndex), "Body JSON", this, itemIndex) as typeof body; validateBodyValue(body, {"name":"bodyJson","displayName":"Body JSON","type":"any","required":true,"description":"Raw request body","items":{"name":"item","displayName":"Item","type":"object","fields":[{"name":"prospects","displayName":"Prospects","type":"array","description":"Ids of the prospects","example":["2dP27N0gZ4","bzZWZpl4wM","2dP27NrgZ3"],"items":{"name":"item","displayName":"Item","type":"number"},"representation":"raw"},{"name":"prospectsEmails","displayName":"Prospects Emails","type":"array","description":"Email addresses associated with the prospects","example":["example2@example.com","example1@example.com"],"items":{"name":"item","displayName":"Item","type":"string"},"representation":"raw"},{"name":"tags","displayName":"Tags","type":"string","required":true,"description":"Name of the tag","example":"tag 1"}],"representation":"raw"},"representation":"raw"}, "Body JSON", this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Error codes: 40501 invalid prospects; 40502 invalid prospect emails; 40503 provide at least one of prospects or prospect emails."},"403":{"title":"4003: You don't have required permissions to perform this task<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceContactController_unsubscribeProspects": {
        
        
        const path = "/v1/contacts/unsubscribe";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"403":{"title":"4003: You don't have required permissions to perform this task<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceContactController_unsubscribeProspects-postV1ProspectsUnsubscribe": {
        
        
        const path = "/v1/prospects/unsubscribe";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"prospectIds","displayName":"Prospect Ids","description":"array of prospect ids to unsubscribe","type":"array","required":true,"example":["2dP27N0gZ4","bzZWZpl4wM"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("prospectIds", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"403":{"title":"4003: You don't have required permissions to perform this task<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceContactController_updateProspects": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/prospects/status";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["pauseDelayInDays"] !== undefined) setBodyField(body as IDataObject, {"name":"pauseDelayInDays","displayName":"Pause Delay In Days","description":"optional property to define pause delay in days in case of pausing prospects, only used when status=paused","type":"number"}, additionalFields["pauseDelayInDays"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"prospectAndSequenceIds","displayName":"Prospect And Sequence Ids","description":"array of prospect id and step id","type":"array","required":true,"example":[{"prospectId":"2dP27N0gZ4","sequenceId":"VMw56r9jPb"}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"prospectId","displayName":"Prospect Id","description":"prospect id of an existing prospect","type":"string","required":true,"example":"2dP27N0gZ4"},{"name":"sequenceId","displayName":"Sequence Id","description":"sequence id in which the related prospect id exists","type":"string","required":true,"example":"VMw56r9jPb"}]}}, this.getNodeParameter("prospectAndSequenceIds", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"status","displayName":"Status","description":"status type to update","type":"string","required":true}, this.getNodeParameter("status", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Could not change prospects status<br><br>4001: Invalid sequence id or prospect id<br><br>"},"403":{"title":"4003: You don't have required permissions to perform this task<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "ScheduleController_createSchedule": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/schedules";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["isDefault"] !== undefined) setBodyField(body as IDataObject, {"name":"isDefault","displayName":"Is Default","description":"If true, mark this schedule as the account default. New sequences without an explicit scheduleId will use this schedule.","type":"boolean","example":false}, additionalFields["isDefault"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"name","displayName":"Name","description":"Human-readable label for the schedule.","type":"string","required":true,"example":"Weekdays 9-5"}, this.getNodeParameter("name", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"timeSlots","displayName":"Time Slots","description":"Exactly 7 entries — one per day of the week (Sunday through Saturday). Days with no sending windows should have an empty <code>slots</code> array.","type":"array","required":true,"example":[{"day":0,"slots":[]},{"day":1,"slots":[{"end":{"hour":17,"minute":0},"start":{"hour":9,"minute":0}}]},{"day":2,"slots":[{"end":{"hour":17,"minute":0},"start":{"hour":9,"minute":0}}]},{"day":3,"slots":[{"end":{"hour":17,"minute":0},"start":{"hour":9,"minute":0}}]},{"day":4,"slots":[{"end":{"hour":17,"minute":0},"start":{"hour":9,"minute":0}}]},{"day":5,"slots":[{"end":{"hour":17,"minute":0},"start":{"hour":9,"minute":0}}]},{"day":6,"slots":[]}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"day","displayName":"Day","description":"0=Sunday, 1=Monday, 2=Tuesday, 3=Wednesday, 4=Thursday, 5=Friday, 6=Saturday","type":"number","required":true,"minValue":0,"maxValue":6,"example":1},{"name":"slots","displayName":"Slots","description":"Sending windows for this day. Pass an empty array to mark the day as inactive.","type":"array","required":true,"example":[{"end":{"hour":17,"minute":0},"start":{"hour":9,"minute":0}}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"end","displayName":"End","description":"End time of the sending window.","type":"object","required":true,"example":{"hour":17,"minute":0},"representation":"raw","fields":[{"name":"hour","displayName":"Hour","type":"number","required":true,"minValue":0,"maxValue":23,"example":9},{"name":"minute","displayName":"Minute","type":"number","required":true,"minValue":0,"maxValue":59,"example":0}]},{"name":"start","displayName":"Start","description":"Start time of the sending window.","type":"object","required":true,"example":{"hour":9,"minute":0},"representation":"raw","fields":[{"name":"hour","displayName":"Hour","type":"number","required":true,"minValue":0,"maxValue":23,"example":9},{"name":"minute","displayName":"Minute","type":"number","required":true,"minValue":0,"maxValue":59,"example":0}]}]}}]}}, this.getNodeParameter("timeSlots", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"timezone","displayName":"Timezone","description":"IANA timezone identifier (e.g. <code>America/New_York</code>, <code>Asia/Kolkata</code>). Must be a valid entry from the IANA Time Zone Database.","type":"string","required":true,"example":"America/New_York"}, this.getNodeParameter("timezone", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to create schedule<br><br>"}};
        break;
      }
    case "ScheduleController_getSchedules": {
        
        
        const path = "/v1/schedules";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "SequenceController_addContactsToSequence": {
        
        
        let path = "/v1/sequences/{sequenceId}/contacts";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
        setBodyField(body as IDataObject, {"name":"contactIds","displayName":"Contact Ids","description":"Contact IDs to add","type":"array","required":true,"example":["2dP27N0gZ4","VMw56r9jPb"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("contactIds", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"stepId","displayName":"Step Id","description":"Sequence step ID","type":"string","required":true,"example":"bwOLEx4l8G"}, this.getNodeParameter("stepId", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to add contacts to sequence<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceController_addEmailAccountToSequence": {
        
        
        let path = "/v1/sequences/{sequenceId}/email-accounts/add";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
        setBodyField(body as IDataObject, {"name":"emailAccountIds","displayName":"Email Account Ids","description":"Array of email account IDs to be added.","type":"array","required":true,"example":["1Gz3xlNwr9","ajzR8xpPAq","vXwAZr6P8q"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"number"}}, this.getNodeParameter("emailAccountIds", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Error codes: 40100 sequence not found; 40105 email account already attached; 40200 email account not found; 40201 email account inactive."},"413":{"title":"40003: The maximum email account limit has been exceeded. You can add up to 50 email accounts at a time<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceController_createSequence": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/sequences";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["emailAccountIds"] !== undefined) setBodyField(body as IDataObject, {"name":"emailAccountIds","displayName":"Email Account Ids","description":"Array of hashed email account IDs to attach as senders of the sequence. If any ID is invalid, inactive, or exceeds the account limit, the entire request is rejected and no sequence is created.","type":"array","example":["1Gz3xlNwr9","ajzR8xpPAq"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["emailAccountIds"], this, itemIndex);
    if (additionalFields["scheduleId"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleId","displayName":"Schedule Id","description":"Hashed ID of the sending schedule to assign to the sequence.","type":"string"}, additionalFields["scheduleId"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"title","displayName":"Title","type":"string","required":true}, this.getNodeParameter("title", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"4000: Failed to create sequence<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceController_createStep": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/sequences/{sequenceId}/steps";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
        setBodyField(body as IDataObject, {"name":"absoluteDays","displayName":"Absolute Days","description":"The absolute number of days for the sequence step.","type":"number","required":true,"minValue":1,"maxValue":999,"example":3}, this.getNodeParameter("absoluteDays", itemIndex), this, itemIndex);
    if (additionalFields["assigneeId"] !== undefined) setBodyField(body as IDataObject, {"name":"assigneeId","displayName":"Assignee Id","description":"User ID of the assignee who will be assigned the tasks generated by this step, default = User ID of sequence owner","type":"string","example":"z6R8Mw4vBn"}, additionalFields["assigneeId"], this, itemIndex);
    if (additionalFields["priority"] !== undefined) setBodyField(body as IDataObject, {"name":"priority","displayName":"Priority","description":"Priority of the tasks that will be generated by this step, default is NORMAL","type":"string","enum":["Urgent","High","Normal","Low"],"example":3}, additionalFields["priority"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"type","displayName":"Type","description":"The task 'type' of step that will be created","type":"string","required":true,"enum":["Email","LinkedInConnectionRequest","LinkedInMessage","LinkedInInMail","LinkedInViewProfile","LinkedInPostInteration","Custom","CallIntroduction","CallDemo","CallFollowUp","CallReminder","CallOther","WhatsappMessage","WhatsappVoiceMessage","WhatsappVoiceCall"],"example":1}, this.getNodeParameter("type", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"variants","displayName":"Variants","description":"Email steps support up to 26 variants for A/B testing; other channels use one. Payload fields depend on the channel. Email uses subject, content, and optional preheader; task-based channels may use taskNote, but email steps do not.","type":"array","required":true,"example":[{"attachmentIds":["lN5xKp2vJq"],"payload":{"content":"<p>Hi {{firstName}},</p><p>Hope you're doing well — wanted to reach out about {{companyName}}.</p><p>Best,<br/>Vatsal</p>","preheader":"Quick question about your team","subject":"Quick question, {{firstName}}"}}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"attachmentIds","displayName":"Attachment Ids","description":"Hashed IDs of attachments to include on this variant (Email steps only). Upload via POST /v1/attachments to obtain an ID. IDs are decoded to numbers by the gateway before forwarding to the edge service.","type":"array","example":["lN5xKp2vJq","aQ8dVzYw1R"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"payload","displayName":"Payload","description":"Payload for the variant based on the step type","type":"object","representation":"raw","fields":[{"name":"action","displayName":"Action","description":"Custom task action","type":"object","example":{"name":"Visit Example Website","url":"https://example.com"},"representation":"raw"},{"name":"connectionNote","displayName":"Connection Note","description":"LinkedIn connection note","type":"string","example":"LinkedIn Connection Note"},{"name":"content","displayName":"Content","description":"Email content (HTML)","type":"string","example":"<div><span style=\"font-family: sans-serif; font-size: 13px;\">Sample email content</span></div>"},{"name":"message","displayName":"Message","description":"LinkedIn InMail or WhatsApp message","type":"string","example":"Sample LinkedIn InMail message"},{"name":"preheader","displayName":"Preheader","description":"Email preheader text","type":"string","example":"Sample preheader"},{"name":"subject","displayName":"Subject","description":"Email subject line","type":"string","example":"Sample email subject"},{"name":"title","displayName":"Title","description":"Custom task title","type":"string","example":"Sample Custom Title"}]},{"name":"taskNote","displayName":"Task Note","description":"Default task note for tasks generated by this variant. NOT allowed on Email variants (type=1) — edge rejects it because Email is automated, not task-based. Use for LinkedIn / Call / Task / WhatsApp channels.","type":"string","example":"Follow up within 24 hours if no reply"}]}}, this.getNodeParameter("variants", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["absoluteDays","id","name","number","type"], simplified: ["absoluteDays","id","name","number","type"] };
        errorPlan = {"400":{"title":"4000: Failed to create sequence step<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceController_createVariant": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/sequences/{sequenceId}/steps/{stepId}/variants";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
    path = path.split("{stepId}").join(encodeURIComponent(String(this.getNodeParameter("stepId", itemIndex))));
        if (additionalFields["absoluteDays"] !== undefined) setBodyField(body as IDataObject, {"name":"absoluteDays","displayName":"Absolute Days","description":"Override day number for this variant. Defaults to the step's absoluteDays.","type":"number","minValue":1,"example":3}, additionalFields["absoluteDays"], this, itemIndex);
    if (additionalFields["assigneeId"] !== undefined) setBodyField(body as IDataObject, {"name":"assigneeId","displayName":"Assignee Id","description":"Hashed user ID to assign generated tasks to (task-based channels). Defaults to the sequence owner.","type":"string","example":"z6R8Mw4vBn"}, additionalFields["assigneeId"], this, itemIndex);
    if (additionalFields["attachmentIds"] !== undefined) setBodyField(body as IDataObject, {"name":"attachmentIds","displayName":"Attachment Ids","description":"Hashed IDs of attachments to include (Email variants only). Upload via POST /v1/attachments to obtain an ID.","type":"array","example":["lN5xKp2vJq"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["attachmentIds"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"payload","displayName":"Payload","description":"Variant payload by channel: Email uses subject, content, and optional preheader; LinkedIn connection requests use connectionNote; LinkedIn messages use message; InMail uses subject and message; view-profile, post-interaction, task, call, and WhatsApp steps use an empty object.","type":"object","required":true,"example":{"content":"<p>Hi {{firstName}},</p><p>Wanted to follow up on my last email.</p>","preheader":"Following up","subject":"Following up, {{firstName}}"},"representation":"raw"}, this.getNodeParameter("payload", itemIndex), this, itemIndex);
    if (additionalFields["priority"] !== undefined) setBodyField(body as IDataObject, {"name":"priority","displayName":"Priority","description":"Task priority (1=Urgent, 2=High, 3=Normal, 4=Low). Used when the channel generates a task.","type":"string","enum":["Urgent","High","Normal","Low"],"example":3}, additionalFields["priority"], this, itemIndex);
    if (additionalFields["taskNote"] !== undefined) setBodyField(body as IDataObject, {"name":"taskNote","displayName":"Task Note","description":"Task note for the generated task. NOT allowed on Email variants (type=1) — edge rejects it because Email is automated, not task-based. Use for LinkedIn / Call / Task / WhatsApp channels.","type":"string","example":"Follow up within 24 hours if no reply"}, additionalFields["taskNote"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"type","displayName":"Type","description":"Channel code: 1 Email; 2 LinkedIn connection request; 3 LinkedIn message; 4 LinkedIn InMail; 5 LinkedIn profile view; 6 LinkedIn post interaction; 9 Task; 11 Call introduction; 12 Call demo; 13 Call follow-up; 14 Call reminder; 15 Call other; 16 WhatsApp message; 17 WhatsApp voice message; 18 WhatsApp voice call.","type":"string","required":true,"enum":["Email","LinkedInConnectionRequest","LinkedInMessage","LinkedInInMail","LinkedInViewProfile","LinkedInPostInteration","Custom","CallIntroduction","CallDemo","CallFollowUp","CallReminder","CallOther","WhatsappMessage","WhatsappVoiceMessage","WhatsappVoiceCall"],"example":1}, this.getNodeParameter("type", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "SequenceController_getSequenceSettings": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/sequences/{sequenceId}/settings";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
    if (additionalFields["code"] !== undefined) qs["code"] = additionalFields["code"];
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "SequenceController_getSequenceStepVariants": {
        
        
        let path = "/v1/sequences/{sequenceId}/steps/{stepId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
    path = path.split("{stepId}").join(encodeURIComponent(String(this.getNodeParameter("stepId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["executionType","id","payload","status","stepStatus","taskNote","type"], simplified: ["executionType","id","payload","status","stepStatus","taskNote","type"] };
        errorPlan = {"400":{"title":"4000: Failed to fetch sequence step variants<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceController_getSequenceSteps": {
        
        
        let path = "/v1/sequences/{sequenceId}/steps";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "SequenceController_getSequencesWithSteps": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/sequences";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["sequenceName"] !== undefined) qs["sequenceName"] = additionalFields["sequenceName"];
    if (additionalFields["pageSize"] !== undefined) qs["pageSize"] = additionalFields["pageSize"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["sort"] !== undefined) qs["sort"] = additionalFields["sort"];
    if (additionalFields["sortBy"] !== undefined) qs["sortBy"] = additionalFields["sortBy"];
    if (additionalFields["clientIds"] !== undefined) qs["clientIds"] = additionalFields["clientIds"];
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"4000: Failed to fetch sequences and their steps<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceController_importProspectsV2": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/sequences/prospects/import-with-field-name";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"conflictAction","displayName":"Conflict Action","description":"Action when a prospect already exists in the sequence: overwrite updates supplied fields but leaves blank cells unchanged; noUpdate skips existing prospects; addMissingFields fills only empty fields; upsert updates supplied fields and adds missing ones, with blank cells clearing existing values.","type":"string","required":true,"enum":["overwrite","noUpdate","addMissingFields","upsert"]}, this.getNodeParameter("conflictAction", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"prospectList","displayName":"Prospect List","description":"The array of prospect list. Each prospect is represented by an array of fields.","type":"array","required":true,"example":[{"Company":"ABC","Country":"USA","Email":"johnsmith@example.com","First Name":"Jon","Job Title":"Procurement Manager","Last Name":"Smith","Phone Number":"1234567890"},{"Company":"ABC","Country":"USA","Email":"jemmy@example.com","First Name":"Jemmy","Job Title":"Procurement Manager","Last Name":"Lorance","Phone Number":"1234567891"}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("prospectList", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"stepId","displayName":"Step Id","description":"Set the step to which the prospect is to be added","type":"number","required":true,"example":"0VOLRwYe82"}, this.getNodeParameter("stepId", itemIndex), this, itemIndex);
    if (additionalFields["tags"] !== undefined) setBodyField(body as IDataObject, {"name":"tags","displayName":"Tags","description":"Array of tags to be assigned to the imported prospects. If the tag does not exist, it will be created.","type":"array","example":["tag1","tag2","tag3"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["tags"], this, itemIndex);
    if (additionalFields["verifyProspects"] !== undefined) setBodyField(body as IDataObject, {"name":"verifyProspects","displayName":"Verify Prospects","description":"Optional. Set to \"true\" if prospect's email address should be verified","type":"boolean"}, additionalFields["verifyProspects"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to import prospects<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceController_removeEmailAccountFromSequence": {
        
        
        let path = "/v1/sequences/{sequenceId}/email-accounts/remove";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
        setBodyField(body as IDataObject, {"name":"emailAccountIds","displayName":"Email Account Ids","description":"Array of email account IDs to be removed.","type":"array","required":true,"example":["1Gz3xlNwr9","ajzR8xpPAq","vXwAZr6P8q"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"number"}}, this.getNodeParameter("emailAccountIds", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"<b><i> Error Codes : </i></b><br><br>\n>* <b>Status:</b> 40100 &nbsp;&nbsp;<b>Description:</b> Sequence is not found\n\n>* <b>Status:</b> 40200 &nbsp;&nbsp;<b>Description:</b> Email account is not found"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceController_sendTestEmail": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/sequences/{sequenceId}/test-email";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
        setBodyField(body as IDataObject, {"name":"content","displayName":"Content","description":"HTML body of the test email.","type":"string","required":true,"example":"<p>Hello from OpenAPI validation.</p>"}, this.getNodeParameter("content", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"fromEmailAccountId","displayName":"From Email Account Id","description":"Public sending email account ID. The sender must be connected to the sequence.","type":"string","required":true,"example":"Y8aL4J6jaN"}, this.getNodeParameter("fromEmailAccountId", itemIndex), this, itemIndex);
    if (additionalFields["preheader"] !== undefined) setBodyField(body as IDataObject, {"name":"preheader","displayName":"Preheader","description":"Preview text shown in the recipient inbox.","type":"string","example":"OpenAPI preheader"}, additionalFields["preheader"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"subject","displayName":"Subject","description":"Subject line of the test email.","type":"string","required":true,"example":"OpenAPI test email"}, this.getNodeParameter("subject", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"to","displayName":"To","description":"One or more recipient email addresses for the test email.","type":"array","required":true,"example":["qa.recipient@example.org"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("to", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Please connect email account with this sequence<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceController_sequenceEmailAccountList": {
        
        
        let path = "/v1/sequences/{sequenceId}/email-accounts";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["createdAt","deletedAt","emailAccount","id","sequenceId"], simplified: ["createdAt","deletedAt","emailAccount","id","sequenceId"] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceController_updatePriorityDistribution": {
        
        
        let path = "/v1/sequences/{sequenceId}/priority-distribution";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
        setBodyField(body as IDataObject, {"name":"priorityDistributionId","displayName":"Priority Distribution Id","description":"Priority distribution preset id.\n`1` = Prioritise Follow-Ups (more emails from follow-up steps).\n`2` = Prioritise New Prospects (more emails from the first step).\n`3` = Balanced Sending (equal across steps).\n`4` = Aggressively Prioritise New Prospects (~80% from the first step).","type":"string","required":true,"enum":["PrioritiseFollowUps","PrioritiseNewProspects","BalancedSending","AggressivelyPrioritiseNewProspects"],"example":3}, this.getNodeParameter("priorityDistributionId", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "SequenceController_updateProspectOutcome": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/sequences/update-prospect-outcome";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["dealValue"] !== undefined) setBodyField(body as IDataObject, {"name":"dealValue","displayName":"Deal Value","type":"number"}, additionalFields["dealValue"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"outcomeName","displayName":"Outcome Name","type":"string","required":true}, this.getNodeParameter("outcomeName", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"prospectEmails","displayName":"Prospect Emails","type":"array","required":true,"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("prospectEmails", itemIndex), this, itemIndex);
    if (additionalFields["sequenceId"] !== undefined) setBodyField(body as IDataObject, {"name":"sequenceId","displayName":"Sequence Id","type":"string"}, additionalFields["sequenceId"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceController_updateSequenceSchedule": {
        
        
        let path = "/v1/sequences/{sequenceId}/schedule";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
        setBodyField(body as IDataObject, {"name":"scheduleId","displayName":"Schedule Id","description":"Hashed ID of the schedule to assign to the sequence.","type":"string","required":true,"example":"aB3xK9"}, this.getNodeParameter("scheduleId", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "SequenceController_updateSequenceSettings": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/sequences/{sequenceId}/settings";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
        if (additionalFields["scheduleId"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleId","displayName":"Schedule Id","description":"Hashed ID of the schedule to assign to this sequence.","type":"string"}, additionalFields["scheduleId"], this, itemIndex);
    if (additionalFields["settings"] !== undefined) setBodyField(body as IDataObject, {"name":"settings","displayName":"Settings","description":"Array of settings to update. All codes are forwarded.","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"value","displayName":"Value","description":"Sequence setting values (string values by code):\n1 Unsubscribe link: empty string disables it; otherwise use one of the three presets below verbatim or custom HTML containing {{link}}. The server replaces {{link}} with the recipient's unsubscribe URL.\n- If you'd prefer not to receive email from me, <a href=\"{{link}}\" target=\"_blank\">Unsubscribe here</a>.\n- If you don't want to receive such emails in future, <a href=\"{{link}}\" target=\"_blank\">Unsubscribe here</a>.\n- Not up for all these emails? No Sweat! you can, <a href=\"{{link}}\" target=\"_blank\">Unsubscribe here</a>.\n2 Unsubscribe text: any plain-text string; empty string disables it.\n3 Stop on reply: 1=stop, 0=continue. 4 Track link clicks, 5 track opens, 6 allow risky prospects, 11 match sender and recipient ESP, 13 add a List-Unsubscribe header; each uses 1=on and 0=off.\n7 BCC and 8 CC: JSON-array strings, such as [\"bcc@example.com\"] or [].\n9 Text-only email: 0=off, 1=remove styling, 2=send all emails as text. With 2, codes 4, 5, and 12 must be 0.\n12 First step text-only: 1=on, 0=off. Use 1 when code 9 is 0; use 0 when code 9 is 2.","type":"string","required":true}]}}, additionalFields["settings"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"}};
        break;
      }
    case "SequenceController_updateSequenceStatus": {
        
        
        const path = "/v1/sequences/status";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"sequenceIds","displayName":"Sequence Ids","description":"Ids of the sequences","type":"array","required":true,"example":["2dP27N0gZ4","2dP27NugZ3","2dP27NrgZ3"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"number"}}, this.getNodeParameter("sequenceIds", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"status","displayName":"Status","description":"status","type":"string","required":true,"enum":["resume","pause"],"example":"resume"}, this.getNodeParameter("status", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Error codes: 40100 sequence not found; 40101 sequence already paused; 40102 sequence already active; 40103 no active email account is attached."},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceController_updateVariant": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/sequences/{sequenceId}/steps/{stepId}/variants/{variantId}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
    path = path.split("{stepId}").join(encodeURIComponent(String(this.getNodeParameter("stepId", itemIndex))));
    path = path.split("{variantId}").join(encodeURIComponent(String(this.getNodeParameter("variantId", itemIndex))));
        if (additionalFields["absoluteDays"] !== undefined) setBodyField(body as IDataObject, {"name":"absoluteDays","displayName":"Absolute Days","description":"Override day number for this variant (1–999). Each day can hold at most one Email step.","type":"number","minValue":1,"example":3}, additionalFields["absoluteDays"], this, itemIndex);
    if (additionalFields["assigneeId"] !== undefined) setBodyField(body as IDataObject, {"name":"assigneeId","displayName":"Assignee Id","description":"Hashed user ID to assign generated tasks to (task-based channels). Defaults to the sequence owner.","type":"string","example":"z6R8Mw4vBn"}, additionalFields["assigneeId"], this, itemIndex);
    if (additionalFields["attachmentIds"] !== undefined) setBodyField(body as IDataObject, {"name":"attachmentIds","displayName":"Attachment Ids","description":"Full replacement of the attachment list for this variant (Email variants only). Pass `[]` to remove all attachments. Upload attachments via POST /v1/attachments to obtain an ID.","type":"array","example":["lN5xKp2vJq","aQ8dVzYw1R"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["attachmentIds"], this, itemIndex);
    if (additionalFields["payload"] !== undefined) setBodyField(body as IDataObject, {"name":"payload","displayName":"Payload","description":"Payload for the variant. Shape must match the step's channel — see OpenAPI.md (Email: `{subject, content, preheader?}`, LinkedInConnectionRequest: `{connectionNote}`, LinkedInMessage: `{message}`, LinkedInInMail: `{subject, message}`, ViewProfile/PostInteraction/Task/Call/Whatsapp: `{}`).","type":"object","example":{"content":"<p>Hi {{firstName}},</p><p>Wanted to follow up on my last email.</p>","preheader":"Following up","subject":"Following up, {{firstName}}"},"representation":"raw"}, additionalFields["payload"], this, itemIndex);
    if (additionalFields["priority"] !== undefined) setBodyField(body as IDataObject, {"name":"priority","displayName":"Priority","description":"Task priority (1=Urgent, 2=High, 3=Normal, 4=Low). Used when the channel generates a task.","type":"string","enum":["Urgent","High","Normal","Low"],"example":3}, additionalFields["priority"], this, itemIndex);
    if (additionalFields["status"] !== undefined) setBodyField(body as IDataObject, {"name":"status","displayName":"Status","description":"Variant status: 0 = Inactive (paused), 1 = Active.","type":"number","enum":[0,1],"example":1}, additionalFields["status"], this, itemIndex);
    if (additionalFields["taskNote"] !== undefined) setBodyField(body as IDataObject, {"name":"taskNote","displayName":"Task Note","description":"Task note for the variant. NOT allowed on Email variants (type=1) — edge rejects it because Email is automated, not task-based. Use for LinkedIn / Call / Task / WhatsApp channels.","type":"string","example":"Follow up within 24 hours if no reply"}, additionalFields["taskNote"], this, itemIndex);
    if (additionalFields["type"] !== undefined) setBodyField(body as IDataObject, {"name":"type","displayName":"Type","description":"Channel type of the variant. Rarely changed on update — must still match the parent step.","type":"string","enum":["Email","LinkedInConnectionRequest","LinkedInMessage","LinkedInInMail","LinkedInViewProfile","LinkedInPostInteration","Custom","CallIntroduction","CallDemo","CallFollowUp","CallReminder","CallOther","WhatsappMessage","WhatsappVoiceMessage","WhatsappVoiceCall"],"example":1}, additionalFields["type"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to update sequence step variant<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SequenceController_verifyProspects": {
        
        
        let path = "/v1/sequences/{sequenceId}/verify-prospects";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: No email addresses that can be verified<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "SubsequenceController_createSubsequence": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/sequences/{sequenceId}/subsequence";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
        setBodyField(body as IDataObject, {"name":"conditions","displayName":"Conditions","description":"Trigger conditions (min 1). All conditions use implicit AND logic. Each item is validated against the condition matrix — invalid name/operation/value combinations are rejected with a 400.","type":"array","required":true,"example":[{"name":"Replied","operation":"is","value":false},{"conditionId":"cond-open-01","name":"Email Open Count","operation":"greater than","value":3},{"name":"Link Click","operation":"is any"}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"conditionId","displayName":"Condition Id","description":"Client-assigned identifier for the condition. Any string; auto-generated (11-char hex) when omitted.","type":"string","example":"cond-open-01"},{"name":"name","displayName":"Name","description":"Condition type. Must be one of the 7 valid values defined by <code>ConditionName</code>.","type":"string","required":true,"enum":["Email Open Count","Replied","Reply Body Contains","Link Click","Prospect Outcome","Last Step Completed","Prospect Tag"],"example":"Replied"},{"name":"operation","displayName":"Operation","description":"Operator valid for the selected <code>name</code>. See the condition matrix in the API docs — invalid name/operation combinations are rejected with a 400.","type":"string","required":true,"enum":["greater than","less than","greater than or equals","less than or equals","is","is not","is any","is not any","contains"],"example":"is"},{"name":"value","displayName":"Value","description":"Threshold or filter value. Type depends on <code>name</code> × <code>operation</code>: integer, boolean, string[] (URLs or keywords), or integer[] (IDs). Omit entirely for <code>Link Click</code> with <code>is any</code> / <code>is not any</code>.","type":"alternative","example":false,"composition":"oneOf","representation":"raw","alternatives":[{"name":"alternative1","displayName":"Alternative1","type":"number"},{"name":"alternative2","displayName":"Alternative2","type":"boolean"},{"name":"alternative3","displayName":"Alternative3","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"alternative4","displayName":"Alternative4","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"integer"}}]}]}}, this.getNodeParameter("conditions", itemIndex), this, itemIndex);
    if (additionalFields["firstStepRelativeDays"] !== undefined) setBodyField(body as IDataObject, {"name":"firstStepRelativeDays","displayName":"First Step Relative Days","description":"Days after the trigger fires before the first step runs (entry delay). Minimum 1. When omitted, no delay is applied.","type":"number","minValue":1,"example":2}, additionalFields["firstStepRelativeDays"], this, itemIndex);
    if (additionalFields["scheduleId"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleId","displayName":"Schedule Id","description":"Sending schedule ID (integer). Use <code>GET /v1/schedules</code> to list available IDs. When omitted, the account default schedule is assigned.","type":"number","example":58964}, additionalFields["scheduleId"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"title","displayName":"Title","description":"Subsequence title. 1–200 characters.","type":"string","required":true,"example":"Re-engagement Subsequence"}, this.getNodeParameter("title", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to create subsequence<br><br>1001: Invalid sequence id<br><br>"}};
        break;
      }
    case "SubsequenceController_getSubsequenceSettings": {
        
        
        let path = "/v1/sequences/subsequence/{subsequenceId}/settings";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{subsequenceId}").join(encodeURIComponent(String(this.getNodeParameter("subsequenceId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to fetch subsequence settings<br><br>1001: Invalid subsequence id<br><br>"}};
        break;
      }
    case "SubsequenceController_listSubsequences": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/sequences/{sequenceId}/subsequences";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
    if (additionalFields["progress"] !== undefined) qs["progress"] = additionalFields["progress"];
    if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
    if (additionalFields["owners"] !== undefined) qs["owners"] = additionalFields["owners"];
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to fetch subsequences<br><br>1001: Invalid sequence id<br><br>"}};
        break;
      }
    case "SubsequenceController_updateSubsequence": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/sequences/{sequenceId}/subsequence/{subsequenceId}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{sequenceId}").join(encodeURIComponent(String(this.getNodeParameter("sequenceId", itemIndex))));
    path = path.split("{subsequenceId}").join(encodeURIComponent(String(this.getNodeParameter("subsequenceId", itemIndex))));
        if (additionalFields["conditions"] !== undefined) setBodyField(body as IDataObject, {"name":"conditions","displayName":"Conditions","description":"Replace all current conditions with this new set (full-replacement, not append). Must contain at least 1 item when supplied. Passing <code>conditions: []</code> is silently ignored by the internal API — does NOT clear conditions.","type":"array","example":[{"conditionId":"cond-open-01","name":"Email Open Count","operation":"greater than","value":3},{"name":"Replied","operation":"is","value":false}],"representation":"raw","items":{"name":"item","displayName":"Item","type":"object","representation":"raw","fields":[{"name":"conditionId","displayName":"Condition Id","description":"Client-assigned identifier for the condition. Any string; auto-generated (11-char hex) when omitted.","type":"string","example":"cond-open-01"},{"name":"name","displayName":"Name","description":"Condition type. Must be one of the 7 valid values defined by <code>ConditionName</code>.","type":"string","required":true,"enum":["Email Open Count","Replied","Reply Body Contains","Link Click","Prospect Outcome","Last Step Completed","Prospect Tag"],"example":"Replied"},{"name":"operation","displayName":"Operation","description":"Operator valid for the selected <code>name</code>. See the condition matrix in the API docs — invalid name/operation combinations are rejected with a 400.","type":"string","required":true,"enum":["greater than","less than","greater than or equals","less than or equals","is","is not","is any","is not any","contains"],"example":"is"},{"name":"value","displayName":"Value","description":"Threshold or filter value. Type depends on <code>name</code> × <code>operation</code>: integer, boolean, string[] (URLs or keywords), or integer[] (IDs). Omit entirely for <code>Link Click</code> with <code>is any</code> / <code>is not any</code>.","type":"alternative","example":false,"composition":"oneOf","representation":"raw","alternatives":[{"name":"alternative1","displayName":"Alternative1","type":"number"},{"name":"alternative2","displayName":"Alternative2","type":"boolean"},{"name":"alternative3","displayName":"Alternative3","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}},{"name":"alternative4","displayName":"Alternative4","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"integer"}}]}]}}, additionalFields["conditions"], this, itemIndex);
    if (additionalFields["firstStepRelativeDays"] !== undefined) setBodyField(body as IDataObject, {"name":"firstStepRelativeDays","displayName":"First Step Relative Days","description":"Replace the entry delay (days after the trigger before the first step runs). Minimum 1.","type":"number","minValue":1,"example":3}, additionalFields["firstStepRelativeDays"], this, itemIndex);
    if (additionalFields["scheduleId"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleId","displayName":"Schedule Id","description":"Replace the current sending schedule. Use <code>GET /v1/schedules</code> to list available IDs.","type":"number","example":60030}, additionalFields["scheduleId"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to update subsequence<br><br>1001: Invalid sequence id<br><br>1001: Invalid subsequence id<br><br>"}};
        break;
      }
    case "TaskController_bulkSkipTask": {
        
        
        const path = "/v1/tasks/bulk-skip";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"taskIds","displayName":"Task Ids","description":"Array of task IDs to skip","type":"array","required":true,"example":["lP0Zg2keMz","MP0Cg1keMa","BS0Zg5keMq"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("taskIds", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["bulkActionId","message","statusCheckUrl"], simplified: ["bulkActionId","message","statusCheckUrl"] };
        errorPlan = {"400":{"title":"4000: Failed to bulk skip tasks<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "TaskController_bulkSnoozeTask": {
        
        
        const path = "/v1/tasks/bulk-snooze";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"snoozeUntil","displayName":"Snooze Until","description":"Date and time until which the tasks should be snoozed","type":"string","format":"date-time","required":true,"example":"2024-06-17T05:55:38.434Z"}, this.getNodeParameter("snoozeUntil", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"taskIds","displayName":"Task Ids","description":"Array of task IDs to snooze","type":"array","required":true,"example":["lP0Zg2keMz","MP0Cg1keMa","BS0Zg5keMq"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("taskIds", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["bulkActionId","message","statusCheckUrl"], simplified: ["bulkActionId","message","statusCheckUrl"] };
        errorPlan = {"400":{"title":"4000: Failed to bulk snooze tasks<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "TaskController_completeTask": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/tasks/{taskId}/complete";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{taskId}").join(encodeURIComponent(String(this.getNodeParameter("taskId", itemIndex))));
        if (additionalFields["callOutcome"] !== undefined) setBodyField(body as IDataObject, {"name":"callOutcome","displayName":"Call Outcome","description":"Call outcome for call tasks (mandatory for call tasks)","type":"string","enum":["Interested","Callback Later","Followup","Not Interested","Wrong Number","No Answer","Left Voicemail","Not In Service"],"example":"no-answer"}, additionalFields["callOutcome"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to complete task<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "TaskController_createTask": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/tasks/create";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["dueDate"] !== undefined) setBodyField(body as IDataObject, {"name":"dueDate","displayName":"Due Date","description":"The due date for the task in YYYY-MM-DD format (e.g. \"2026-04-22\"). Defaults to today if not provided.","type":"string","example":"2026-04-22"}, additionalFields["dueDate"], this, itemIndex);
    if (additionalFields["priority"] !== undefined) setBodyField(body as IDataObject, {"name":"priority","displayName":"Priority","description":"The priority level of the task. Accepted values: \"1\" = Urgent, \"2\" = High, \"3\" = Medium, \"4\" = Low. Defaults to no priority if not provided.","type":"string","enum":["1","2","3","4"],"example":"3"}, additionalFields["priority"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"prospectIds","displayName":"Prospect Ids","description":"List of prospect IDs to create the task for. Pass a single-element array for one prospect, or multiple IDs to create the same task for several prospects in one request.","type":"array","required":true,"example":["8PvBmrB7P7"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("prospectIds", itemIndex), this, itemIndex);
    if (additionalFields["taskNote"] !== undefined) setBodyField(body as IDataObject, {"name":"taskNote","displayName":"Task Note","description":"An optional note or description to attach to the task.","type":"string","example":"Call to follow up on the proposal sent yesterday."}, additionalFields["taskNote"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"taskType","displayName":"Task Type","description":"Task type code: 2 LinkedIn connection request; 3 LinkedIn message; 4 LinkedIn InMail; 5 LinkedIn profile view; 6 LinkedIn post interaction; 9 Custom; 11 Call introduction; 12 Call demo; 13 Call follow-up; 14 Call reminder; 15 Call other; 16 WhatsApp message; 17 WhatsApp voice message; 18 WhatsApp voice call.","type":"string","required":true,"enum":["2","3","4","5","6","9","11","12","13","14","15","16","17","18"],"example":"13"}, this.getNodeParameter("taskType", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "TaskController_getAssigneeList": {
        
        
        const path = "/v1/tasks/assignee/list";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "TaskController_getBulkTaskStatus": {
        
        
        let path = "/v1/tasks/bulk-status/{bulkActionId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{bulkActionId}").join(encodeURIComponent(String(this.getNodeParameter("bulkActionId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["action","bulkActionId","completed","completedAt","createdAt","csvDownloadLink","failed","pending","status","totalTasks"], simplified: ["action","bulkActionId","completed","completedAt","createdAt","csvDownloadLink","failed","pending","status","totalTasks"] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "TaskController_getTaskById": {
        
        
        let path = "/v1/tasks/{taskId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{taskId}").join(encodeURIComponent(String(this.getNodeParameter("taskId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to fetch task by ID<br><br>"},"404":{"title":"The requested task could not be found"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "TaskController_getTaskCounts": {
        
        
        const path = "/v1/tasks/counts";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "TaskController_getTasks": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/tasks";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["pageSize"] !== undefined) qs["pageSize"] = additionalFields["pageSize"];
    if (additionalFields["sortOrder"] !== undefined) qs["sortOrder"] = additionalFields["sortOrder"];
    if (additionalFields["sortBy"] !== undefined) qs["sortBy"] = additionalFields["sortBy"];
    if (additionalFields["status"] !== undefined) qs["status"] = additionalFields["status"];
    if (additionalFields["sequenceId"] !== undefined) qs["sequenceId"] = additionalFields["sequenceId"];
    if (additionalFields["taskAssignee"] !== undefined) qs["taskAssignee"] = additionalFields["taskAssignee"];
    if (additionalFields["prospectOutcome"] !== undefined) qs["prospectOutcome"] = additionalFields["prospectOutcome"];
    if (additionalFields["taskType"] !== undefined) qs["taskType"] = additionalFields["taskType"];
    if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
    if (additionalFields["callOutcome"] !== undefined) qs["callOutcome"] = additionalFields["callOutcome"];
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["callOutcome","page","pageSize","prospectOutcome","search","sequenceId","sortBy","sortOrder","status","taskAssignee","taskType"], simplified: ["status","callOutcome","page","pageSize","prospectOutcome","search","sequenceId","sortBy","sortOrder","taskAssignee"] };
        errorPlan = {"400":{"title":"4000: Failed to fetch tasks<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "TaskController_skipTask": {
        
        
        let path = "/v1/tasks/{taskId}/skip";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{taskId}").join(encodeURIComponent(String(this.getNodeParameter("taskId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to skip task<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "TaskController_snoozeTask": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/v1/tasks/{taskId}/snooze";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{taskId}").join(encodeURIComponent(String(this.getNodeParameter("taskId", itemIndex))));
        if (additionalFields["callOutcome"] !== undefined) setBodyField(body as IDataObject, {"name":"callOutcome","displayName":"Call Outcome","description":"Call outcome for call tasks (optional)","type":"string","enum":["Interested","Callback Later","Followup","Not Interested","Wrong Number","No Answer","Left Voicemail","Not In Service"],"example":"no-answer"}, additionalFields["callOutcome"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"snoozeUntil","displayName":"Snooze Until","description":"Date and time until which the task should be snoozed","type":"string","required":true,"example":"2024-06-17T05:55:38.434Z"}, this.getNodeParameter("snoozeUntil", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to snooze task<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "TaskController_updateTaskNote": {
        
        
        let path = "/v1/tasks/{taskId}/note";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{taskId}").join(encodeURIComponent(String(this.getNodeParameter("taskId", itemIndex))));
        setBodyField(body as IDataObject, {"name":"note","displayName":"Note","description":"Note content for the task","type":"string","required":true,"example":"Mention the new product updates in the call"}, this.getNodeParameter("note", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"4000: Failed to update task note<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "UniboxController_getCategories": {
        
        
        const path = "/v1/unibox/categories";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "UnifiedInboxController_getAll": {
        
        
        const path = "/v1/unified-inbox/unread-email-threads-count";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "UnifiedInboxController_getAllEmails": {
        
        
        let path = "/v1/unified-inbox/emails/{emailThreadId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{emailThreadId}").join(encodeURIComponent(String(this.getNodeParameter("emailThreadId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"5001: Internal server error, please try again later<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "UnifiedInboxController_getEmailContentForSingleEmail": {
        
        
        let path = "/v1/unified-inbox/emails/{emailThreadId}/{emailId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{emailThreadId}").join(encodeURIComponent(String(this.getNodeParameter("emailThreadId", itemIndex))));
    path = path.split("{emailId}").join(encodeURIComponent(String(this.getNodeParameter("emailId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"5001: Internal server error, please try again later<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "UnifiedInboxController_getEmailList": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/unified-inbox/emails";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["categoryIds"] !== undefined) setBodyField(body as IDataObject, {"name":"categoryIds","displayName":"Category Ids","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["categoryIds"], this, itemIndex);
    if (additionalFields["clientIds"] !== undefined) setBodyField(body as IDataObject, {"name":"clientIds","displayName":"Client Ids","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["clientIds"], this, itemIndex);
    if (additionalFields["emailAccountIds"] !== undefined) setBodyField(body as IDataObject, {"name":"emailAccountIds","displayName":"Email Account Ids","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["emailAccountIds"], this, itemIndex);
    if (additionalFields["endDate"] !== undefined) setBodyField(body as IDataObject, {"name":"endDate","displayName":"End Date","type":"string"}, additionalFields["endDate"], this, itemIndex);
    if (additionalFields["isRead"] !== undefined) setBodyField(body as IDataObject, {"name":"isRead","displayName":"Is Read","type":"number"}, additionalFields["isRead"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"limit","displayName":"Limit","type":"number","required":true}, this.getNodeParameter("limit", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"owners","displayName":"Owners","type":"array","required":true,"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("owners", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"page","displayName":"Page","type":"number","required":true}, this.getNodeParameter("page", itemIndex), this, itemIndex);
    if (additionalFields["search"] !== undefined) setBodyField(body as IDataObject, {"name":"search","displayName":"Search","type":"string","default":""}, additionalFields["search"], this, itemIndex);
    if (additionalFields["sentiment"] !== undefined) setBodyField(body as IDataObject, {"name":"sentiment","displayName":"Sentiment","description":"Filter by SentimentsType","type":"string","enum":["Positive","Negative","Neutral","Uncategorized"]}, additionalFields["sentiment"], this, itemIndex);
    if (additionalFields["sequenceIds"] !== undefined) setBodyField(body as IDataObject, {"name":"sequenceIds","displayName":"Sequence Ids","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["sequenceIds"], this, itemIndex);
    if (additionalFields["startDate"] !== undefined) setBodyField(body as IDataObject, {"name":"startDate","displayName":"Start Date","type":"string"}, additionalFields["startDate"], this, itemIndex);
    if (additionalFields["type"] !== undefined) setBodyField(body as IDataObject, {"name":"type","displayName":"Type","description":"Filter by emails type","type":"string","enum":["system","external"]}, additionalFields["type"], this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"5001: Internal server error, please try again later<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "UnifiedInboxController_getFields": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/unified-inbox/outcome";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["pageSize"] !== undefined) qs["pageSize"] = additionalFields["pageSize"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    qs["category"] = this.getNodeParameter("category", itemIndex);
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["message","payload"], simplified: ["message","payload"] };
        errorPlan = {"400":{"title":"4000: Failed to fetch the outcome<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "UnifiedInboxController_replyOnEmail": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/unified-inbox/emails/reply";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["attachmentIds"] !== undefined) setBodyField(body as IDataObject, {"name":"attachmentIds","displayName":"Attachment Ids","description":"Hashed attachment id(s) returned by POST /v1/attachments.","type":"array","example":["Y8aLkbK0PN"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["attachmentIds"], this, itemIndex);
    if (additionalFields["bcc"] !== undefined) setBodyField(body as IDataObject, {"name":"bcc","displayName":"Bcc","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["bcc"], this, itemIndex);
    if (additionalFields["cc"] !== undefined) setBodyField(body as IDataObject, {"name":"cc","displayName":"Cc","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["cc"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"content","displayName":"Content","type":"string","required":true}, this.getNodeParameter("content", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"emailId","displayName":"Email Id","description":"Hashed email id from the Unified Inbox thread response.","type":"string","required":true,"example":"EaxmPl3Id"}, this.getNodeParameter("emailId", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"emailThreadId","displayName":"Email Thread Id","description":"Hashed Unified Inbox thread id. For webhook flows, use conversationId from the webhook payload as this value.","type":"string","required":true,"example":"Rxh247rdgp"}, this.getNodeParameter("emailThreadId", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"templateId","displayName":"Template Id","type":"number","required":true}, this.getNodeParameter("templateId", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"to","displayName":"To","type":"array","required":true,"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("to", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"unifiedScheduledId","displayName":"Unified Scheduled Id","type":"number","required":true}, this.getNodeParameter("unifiedScheduledId", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"5001: Internal server error, please try again later<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "UserController_getTeamAndMemberList": {
        
        
        const path = "/v1/user/team-member-list";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {};
        break;
      }
    case "WebhookController_createWebhook": {
        
        
        const path = "/v1/webhook/create";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"headers","displayName":"Headers","description":"Custom HTTP headers included in each webhook delivery. Pass `{}` if no custom headers are needed.","type":"object","required":true,"example":{"X-Webhook-Source":"saleshandy"},"representation":"raw"}, this.getNodeParameter("headers", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"name","displayName":"Name","description":"Display name for the webhook.","type":"string","required":true,"example":"Email Sent Webhook"}, this.getNodeParameter("name", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"webhookEvent","displayName":"Webhook Event","description":"Event types to subscribe to. Choose one or more supported values.","type":"array","required":true,"example":["email-sent"],"representation":"raw","items":{"name":"item","displayName":"Item","type":"string","enum":["email-sent","email-opened","email-link-clicked","reply-received","email-bounced","prospect-unsubscribed","prospect-finished","prospect-outcome-updated","sequence-paused","email-account-disconnected","email-account-paused","inbox-analyser-test-completed","task-created","bulk-task-updated","call-task-updated","linkedin-task-updated","custom-task-updated","whatsapp-task-updated","task-updated"]}}, this.getNodeParameter("webhookEvent", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"webhookUrl","displayName":"Webhook Url","description":"Destination URL where the webhook payload will be delivered.","type":"string","required":true,"example":"https://example.org/webhooks/email-sent"}, this.getNodeParameter("webhookUrl", itemIndex), this, itemIndex);
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "WebhookController_getWebhook": {
        
        
        let path = "/v1/webhook/{webhookId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{webhookId}").join(encodeURIComponent(String(this.getNodeParameter("webhookId", itemIndex))));
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
    case "WebhookController_listWebhooks": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/v1/webhook";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["pageSize"] !== undefined) qs["pageSize"] = additionalFields["pageSize"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
        
        
        const serverBaseUrl = resolveServerBaseUrl(this as never, [{"id":"selfHosted","url":"{baseUrl}","kind":"selfHosted","variables":[{"name":"baseUrl","default":"https://api.example.com","enum":[]}]}], "selfHosted", nodeOptions, false);
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"saleshandyApi","type":"apiKey","location":"header","parameter":"x-api-key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"1001: Invalid token<br><br>"},"500":{"title":"5001: Internal server error, please try again later<br><br>"}};
        break;
      }
          default: throw new NodeOperationError(this.getNode(), `Unsupported operation ${operation} for node version ${nodeVersion}`, { itemIndex });
        }
        const returnAll = pagination.style !== 'none' ? Boolean(nodeOptions.returnAll ?? false) : false;
    const resultLimit = pagination.style !== 'none' && !returnAll ? Number(nodeOptions.resultLimit ?? 50) : Math.min(pagination.maxItems, Number.POSITIVE_INFINITY);
    const pageStartTime = Date.now();
    const seenCursors = new Map<string, number>(); const seenPages = new Map<string, number>();
    let page = 1; let offset = 0; let cursor: unknown; let pagesFetched = 0; let estimatedBytes = 0; let finished = false;
    while (!finished && output.length - outputStart < resultLimit && pagesFetched < pagination.maxPages) {
      if (Date.now() - pageStartTime > pagination.maxElapsedMs) throw new NodeOperationError(this.getNode(), 'Pagination elapsed-time budget was exceeded', { itemIndex });
      const qs = options.qs as IDataObject;
      // Only the paginator's own page size is written here. It used to overwrite a
      // limit parameter the operation itself declared and the user had just set.
      if (pagination.limit && (pagesFetched > 0 || qs[pagination.limit] === undefined)) qs[pagination.limit] = Math.min(pagination.pageSize, resultLimit - (output.length - outputStart));
      if (pagination.style === 'offset' && pagination.page) qs[pagination.page] = offset;
      if (pagination.style === 'pageNumber' && pagination.page) qs[pagination.page] = page;
      if (pagination.style === 'cursor' && pagination.cursor && cursor) qs[pagination.cursor] = cursor as string;
      const response = await requestWithRetry(this as never, options, credentialApplications, retryContract, itemIndex);
      pagesFetched += 1;
      const pageFingerprint = JSON.stringify(response);
      const pageRepeats = (seenPages.get(pageFingerprint) ?? 0) + 1;
      seenPages.set(pageFingerprint, pageRepeats);
      if (pageRepeats > pagination.repeatedPageLimit) throw new NodeOperationError(this.getNode(), 'Pagination repeated-page budget was exceeded', { itemIndex });
      estimatedBytes += pageFingerprint.length;
      if (estimatedBytes > pagination.maxMemoryBytes) throw new NodeOperationError(this.getNode(), 'Pagination memory budget was exceeded', { itemIndex });
      if (responsePlan.binary) {
        const binaryPayload = responsePlan.full ? ((response as IDataObject).body ?? response) : response;
        const responseHeaders = (responsePlan.full ? ((response as IDataObject).headers as IDataObject | undefined) : undefined) ?? {};
        const contentType = String(responseHeaders['content-type'] ?? '').split(';')[0].trim() || 'application/octet-stream';
        // prepareBinaryData is what fills in fileName, fileSize and fileExtension.
        // Hand-building the binary entry produced items that downstream nodes could
        // not name or type, and discarded the response's own content type.
        const binaryData = await this.helpers.prepareBinaryData(Buffer.from(binaryPayload as ArrayBuffer), undefined, contentType);
        output.push({ json: {}, binary: { data: binaryData }, pairedItem: { item: itemIndex } });
        finished = true;
        continue;
      }
      const normalizedResponse = responsePlan.full ? ((response as IDataObject).body ?? response) : response;
      const envelopeValue = valueAtPath(normalizedResponse, responsePlan.envelopePath);
      if (responsePlan.envelopePath && envelopeValue === undefined) throw new NodeOperationError(this.getNode(), `Response envelope path "${responsePlan.envelopePath}" was not found`, { itemIndex });
      const envelope = (envelopeValue ?? normalizedResponse) as IDataObject;
      const itemPath = pagination.itemPath || responsePlan.itemPath;
      const extractedItems = valueAtPath(envelope, itemPath);
      if (itemPath && extractedItems === undefined) throw new NodeOperationError(this.getNode(), `Response item path "${itemPath}" was not found`, { itemIndex });
      // A DELETE used to be reported as a fixed { deleted: true } with its body
      // thrown away, which lost the deleted representation and the job handle that
      // asynchronous deletes return. The body is used when there is one.
      const deletedFallback = options.method === 'DELETE' && (normalizedResponse === undefined || normalizedResponse === null || normalizedResponse === '' ||
        (typeof normalizedResponse === 'object' && !Array.isArray(normalizedResponse) && Object.keys(normalizedResponse as IDataObject).length === 0));
      const values = deletedFallback
        ? [{ deleted: true }]
        : Array.isArray(extractedItems) ? extractedItems : Array.isArray(normalizedResponse) ? normalizedResponse : [extractedItems ?? envelope];
      const outputMode = responsePlan.fields.length > 10 ? this.getNodeParameter('outputMode', itemIndex, 'simplified') as string : 'raw';
      const selectedFields = outputMode === 'selected' ? this.getNodeParameter('selectedFields', itemIndex, []) as string[] : [];
      for (const value of values) {
        if (output.length - outputStart >= resultLimit) break;
        const fields = outputMode === 'simplified' ? responsePlan.simplified : outputMode === 'selected' ? selectedFields : [];
        output.push({ json: selectResponseFields(value as IDataObject, fields), pairedItem: { item: itemIndex } });
      }
      if (!returnAll || pagination.style === 'none' || values.length === 0) { finished = true; continue; }
      if (pagination.hasMore && envelope[pagination.hasMore] === false) { finished = true; continue; }
      if (pagination.style === 'cursor') {
        cursor = pagination.responseCursor ? valueAtPath(envelope, pagination.responseCursor) : undefined;
        finished = !cursor;
        if (cursor) {
          const key = String(cursor);
          const repeats = (seenCursors.get(key) ?? 0) + 1;
          seenCursors.set(key, repeats);
          if (repeats > pagination.repeatedCursorLimit) throw new NodeOperationError(this.getNode(), 'Pagination repeated-cursor budget was exceeded', { itemIndex });
        }
      }
      if (pagination.advancement === 'offsetByItems') offset += values.length;
      if (pagination.advancement === 'incrementPage') page += 1;
    }
      } catch (error) {
        if (this.continueOnFail()) {
          output.push({ json: { error: (error as Error).message }, pairedItem: { item: itemIndex } });
          continue;
        }
        if (error instanceof NodeApiError) {
          const status = String((error as unknown as { httpCode?: string; cause?: { statusCode?: number } }).httpCode ?? (error as unknown as { cause?: { statusCode?: number } }).cause?.statusCode ?? 'default');
          const planned = errorPlan[status] ?? errorPlan.default;
          if (planned) {
            const parameterHelp = planned.parameter ? `Check the '${planned.parameter}' parameter.` : undefined;
            const description = [planned.recovery, parameterHelp].filter(Boolean).join(' ');
            throw new NodeApiError(this.getNode(), error as unknown as JsonObject, { itemIndex, message: planned.title, description });
          }
        }
        if (error instanceof NodeApiError) throw new NodeApiError(this.getNode(), error as unknown as JsonObject, { itemIndex });
        throw new NodeOperationError(this.getNode(), error as Error, { itemIndex });
      }
    }
    return [output];
  }
}
