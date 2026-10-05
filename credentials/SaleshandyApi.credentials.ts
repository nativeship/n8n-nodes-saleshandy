import { type IAuthenticateGeneric, type Icon, type ICredentialTestRequest, type ICredentialType, type INodeProperties } from "n8n-workflow";

// Generated with ts-morph
export class SaleshandyApi implements ICredentialType {
  name = "saleshandyApi";
  displayName = "Saleshandy API";
  documentationUrl = "https://api.example.com";
  icon: Icon = {
        light: "file:../nodes/Saleshandy/saleshandy.svg",
        dark: "file:../nodes/Saleshandy/saleshandy.dark.svg"
    };
  properties: INodeProperties[] = [
        {
            displayName: "x-api-key",
            name: "secret",
            type: "string",
            typeOptions: {
                password: true
            },
            default: "",
            required: true
        }
    ];
  authenticate: IAuthenticateGeneric = {
        type: "generic",
        properties: {
            headers: {
                "x-api-key": "={{$credentials.secret}}"
            }
        }
    };
  test: ICredentialTestRequest = {
        request: {
            baseURL: "https://api.example.com",
            url: "/v1/clients"
        }
    };
}
