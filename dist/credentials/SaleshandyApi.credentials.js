"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SaleshandyApi = void 0;
class SaleshandyApi {
    constructor() {
        this.name = "saleshandyApi";
        this.displayName = "Saleshandy API";
        this.documentationUrl = "https://api.example.com";
        this.icon = {
            light: "file:../nodes/Saleshandy/saleshandy.svg",
            dark: "file:../nodes/Saleshandy/saleshandy.dark.svg"
        };
        this.properties = [
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
        this.authenticate = {
            type: "generic",
            properties: {
                headers: {
                    "x-api-key": "={{$credentials.secret}}"
                }
            }
        };
        this.test = {
            request: {
                baseURL: "https://api.example.com",
                url: "/v1/clients"
            }
        };
    }
}
exports.SaleshandyApi = SaleshandyApi;
//# sourceMappingURL=SaleshandyApi.credentials.js.map