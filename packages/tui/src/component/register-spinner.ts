import { getComponentCatalogue } from "@opentui/solid/components"
import * as spinnerExtension from "opentui-spinner/solid"

export function registerOpencodeSpinner() {
  if (!getComponentCatalogue().spinner && "registerSpinner" in spinnerExtension) spinnerExtension.registerSpinner()
}
