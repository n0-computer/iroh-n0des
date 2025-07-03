use proc_macro::TokenStream;

/// Marks a function as a simulation test
///
/// This is a simple marker attribute that doesn't transform the code.
/// The external test runner discovers these functions by parsing the AST
/// and looking for this attribute.
#[proc_macro_attribute]
pub fn sim(_args: TokenStream, input: TokenStream) -> TokenStream {
    // Parse and validate the function signature
    let input_fn = syn::parse_macro_input!(input as syn::ItemFn);

    // Validate it's async and returns Result<SimulationBuilder<_>>
    if input_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(input_fn.sig.fn_token, "sim functions must be async")
            .to_compile_error()
            .into();
    }

    // Return original function unchanged
    TokenStream::from(quote::quote! { #input_fn })
}
