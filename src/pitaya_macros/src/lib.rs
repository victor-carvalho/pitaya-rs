use devise::ext::TypeExt;
use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::{quote, quote_spanned};
use syn::{
    parse::Parser, parse_macro_input, punctuated::Punctuated, spanned::Spanned, Expr, ExprCall,
    ExprMethodCall, ItemFn, LitStr, Path,
};

fn get_handler_info_name(method_name: &str) -> Ident {
    Ident::new(
        &format!("pitaya_handler_info_{}", method_name).to_uppercase(),
        Span::call_site(),
    )
}

#[proc_macro]
pub fn handlers(item: TokenStream) -> TokenStream {
    let mut paths = match <Punctuated<Path, syn::Token![,]>>::parse_terminated.parse(item) {
        Ok(p) => p,
        Err(_e) => {
            return TokenStream::from(quote! {
                compiler_error!("expecting a comma separated list of handler functions");
            });
        }
    };

    let mut punct: Punctuated<ExprMethodCall, syn::Token![;]> = Punctuated::new();

    paths.iter_mut().for_each(|p| {
        let seg = p.segments.last().expect("should have segments");
        let handler_info_name = get_handler_info_name(&seg.ident.to_string());
        let call = ExprMethodCall {
            attrs: vec![],
            receiver: Box::new(Expr::Verbatim(quote! { temp_handlers })),
            dot_token: syn::token::Dot {
                spans: [Span::call_site()],
            },
            method: Ident::new("add", Span::call_site()),
            turbofish: None,
            paren_token: syn::token::Paren {
                span: Span::call_site(),
            },
            args: {
                let mut args = Punctuated::new();
                args.push_value(Expr::Verbatim(quote! { &#handler_info_name }));
                args
            },
        };

        punct.push_value(call);
        punct.push_punct(syn::token::Semi {
            spans: [Span::call_site()],
        });
    });

    TokenStream::from(quote! {
        {
            let mut temp_handlers = ::pitaya_core::handler::Handlers::new();
            #punct
            temp_handlers
        }
    })
}

#[proc_macro_attribute]
pub fn protobuf_handler(attrs: TokenStream, item: TokenStream) -> TokenStream {
    common_handler(HandlerSerializer::Protobuf, attrs, item)
}

#[proc_macro_attribute]
pub fn json_handler(attrs: TokenStream, item: TokenStream) -> TokenStream {
    common_handler(HandlerSerializer::Json, attrs, item)
}

#[derive(Debug, Copy, Clone)]
enum HandlerSerializer {
    Json,
    Protobuf,
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum HandlerKind {
    Client,
    Server,
}

struct CompilerError(Span, String);

fn get_inputs_from_fn(
    item_fn: &syn::ItemFn,
) -> Result<Vec<(&syn::Ident, syn::Type, Span)>, CompilerError> {
    let mut inputs = vec![];
    for fn_arg in &item_fn.sig.inputs {
        let span = fn_arg.span();
        let (ident, ty, span) = match fn_arg {
            syn::FnArg::Typed(arg) => match &*arg.pat {
                syn::Pat::Ident(ref pat) => (&pat.ident, &arg.ty, span),
                syn::Pat::Wild(_) => {
                    return Err(CompilerError(
                        span,
                        "handler arguments cannot be ignored".into(),
                    ));
                }
                _ => {
                    return Err(CompilerError(span, "invalid use of pattern".into()));
                }
            },
            _ => {
                return Err(CompilerError(span, "invalid handler argument".into()));
            }
        };

        let ty: &syn::Type = ty.as_ref();
        inputs.push((
            ident,
            <syn::Type as TypeExt>::with_stripped_lifetimes(ty),
            span,
        ));
    }
    Ok(inputs)
}

fn get_deserialize_code(kind: HandlerSerializer, ty: &syn::Type) -> proc_macro2::TokenStream {
    match kind {
        HandlerSerializer::Json => quote! {
            match ::serde_json::from_slice::<#ty>(&req_data) {
                ::core::result::Result::Ok(v) => v,
                ::core::result::Result::Err(_) => {
                    return ::pitaya_core::protos::Response {
                        data: vec![],
                        error: Some(::pitaya_core::Error::InvalidJsonArgument.to_error()),
                    };
                },
            }
        },
        HandlerSerializer::Protobuf => quote! {
            {
                use ::prost::Message;
                match ::prost::Message::decode(req_data.as_ref()) {
                    ::core::result::Result::Ok(v) => v,
                    ::core::result::Result::Err(_) => {
                        return ::pitaya_core::protos::Response {
                            data: vec![],
                            error: Some(::pitaya_core::Error::InvalidProtobufArgument.to_error()),
                        };
                    },
                }
            }
        },
    }
}

fn common_handler(
    handler_serializer: HandlerSerializer,
    attrs: TokenStream,
    item: TokenStream,
) -> TokenStream {
    let item = parse_macro_input!(item as ItemFn);
    let inputs = match get_inputs_from_fn(&item) {
        Ok(i) => i,
        Err(CompilerError(span, e)) => {
            let lit = syn::LitStr::new(&e, Span::call_site());
            return quote_spanned! {span=>
                compile_error!(#lit);
            }
            .into();
        }
    };

    let parser = Punctuated::<syn::Expr, syn::Token![,]>::parse_separated_nonempty;
    let punct = parser.parse(attrs).expect("failed to parse attributes");

    if punct.len() < 2 || punct.len() > 3 {
        return quote! {
            compile_error!("invalid attribute syntax");
        }
        .into();
    }

    let handler_name_lit = match &punct[0] {
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(lit_str),
            ..
        }) => lit_str,
        _ => {
            return quote! {
                compile_error!("invalid attribute syntax");
            }
            .into()
        }
    };

    let handler_kind = match &punct[1] {
        syn::Expr::Path(expr_path) => match expr_path.path.segments.last() {
            Some(last_seg) if last_seg.ident == "client" => HandlerKind::Client,
            Some(last_seg) if last_seg.ident == "server" => HandlerKind::Server,
            _ => {
                return quote_spanned! {expr_path.span()=>
                    compile_error!("invalid handler kind");
                }
                .into();
            }
        },
        _ => {
            return quote! {
                compile_error!("invalid attribute syntax");
            }
            .into();
        }
    };

    let with_args = if punct.len() > 2 {
        match &punct[2] {
            syn::Expr::Path(expr_path) => match expr_path.path.segments.last() {
                Some(last_seg) if last_seg.ident == "with_args" => true,
                _ => {
                    return quote_spanned! {expr_path.span()=>
                        compile_error!("invalid attribute syntax");
                    }
                    .into();
                }
            },
            _ => {
                return quote! {
                    compile_error!("invalid attribute syntax");
                }
                .into();
            }
        }
    } else {
        false
    };

    let method_ident = item.sig.ident.clone();
    let method_name = method_ident.to_string();
    let method_name_lit = LitStr::new(&method_name, Span::call_site());

    let generated_handler = Ident::new(
        &format!("pitaya_handler_{}", &method_name),
        Span::call_site(),
    );

    let handler_info = get_handler_info_name(&method_name);

    let func_call = ExprCall {
        attrs: vec![],
        func: Box::new(Expr::Verbatim(quote! { #method_ident })),
        paren_token: syn::token::Paren {
            span: Span::call_site(),
        },
        args: {
            let mut p = Punctuated::new();

            if handler_kind == HandlerKind::Client {
                p.push_value(syn::Expr::Verbatim(
                    quote! { session.expect("session should not be None") },
                ));
                p.push_punct(syn::token::Comma {
                    spans: [Span::call_site()],
                });
            }

            for (ident, _, _) in inputs.iter().skip(if handler_kind == HandlerKind::Client {
                1
            } else {
                0
            }) {
                p.push_value(syn::Expr::Verbatim(quote! { #ident }));
                p.push_punct(syn::token::Comma {
                    spans: [Span::call_site()],
                });
            }
            p
        },
    };

    let has_return = match item.sig.output {
        syn::ReturnType::Default => false,
        syn::ReturnType::Type(_, ref ty) => {
            if let syn::Type::Tuple(ref tuple) = ty.as_ref() {
                !tuple.elems.is_empty()
            } else {
                true
            }
        }
    };

    if !has_return {
        return quote! { compile_error!("json handlers must return Result<T, E>"); }.into();
    }

    let input_declaration = if with_args {
        let (input_name, ty, _) = if handler_kind == HandlerKind::Client {
            &inputs[1]
        } else {
            &inputs[0]
        };
        let deserialize_code = get_deserialize_code(handler_serializer, ty);
        quote! {
            let #input_name: #ty = #deserialize_code;
        }
    } else {
        quote! {}
    };

    let state_declarations = {
        let mut s = vec![];
        for (ident, ty, _) in &inputs {
            s.push((ident, <syn::Type as TypeExt>::with_stripped_lifetimes(ty)));
        }
        s
    };

    let state_declarations = state_declarations
        .iter()
        .skip(if handler_kind == HandlerKind::Client { 1 } else { 0 })
        .skip(if with_args { 1 } else { 0 })
        .map(|(ident, ty)| {
            quote! {
                let #ident = match <#ty as ::pitaya_core::context::FromContext>::from_context(&ctx) {
                    ::core::result::Result::Ok(s) => s,
                    ::core::result::Result::Err(::pitaya_core::context::NotFound(s)) => {
                        println!("did not find managed state");
                        return ::pitaya_core::protos::Response {
                            data: vec![],
                            error: Some(::pitaya_core::Error::RouteNotFound.to_error()),
                        };
                    }
                };
            }
        })
        .collect::<Vec<_>>();

    let body = match handler_serializer {
        HandlerSerializer::Json => quote! {
            use ::pitaya_core::ToResponseJson;
            use ::pitaya_core::ToError;

            let req_data = match req.msg.as_ref() {
                ::core::option::Option::Some(msg) => msg.data.clone(),
                _ => vec![],
            };

            let fut = async move {
                #input_declaration

                #(#state_declarations)*

                let res = #func_call.await;
                res.to_response_json()
            };
            ::std::boxed::Box::pin(fut)
        },
        HandlerSerializer::Protobuf => quote! {
            use ::pitaya_core::ToResponseProto;
            use ::pitaya_core::ToError;

            let req_data = match req.msg.as_ref() {
                ::core::option::Option::Some(msg) => msg.data.clone(),
                _ => vec![],
            };

            let fut = async move {
                #input_declaration

                #(#state_declarations)*

                let res = #func_call.await;
                res.to_response_proto()
            };
            ::std::boxed::Box::pin(fut)
        },
    };

    let output = quote! {
        #item

        const #handler_info: ::pitaya_core::handler::StaticHandlerInfo = ::pitaya_core::handler::StaticHandlerInfo {
            handler_name: #handler_name_lit,
            method_name: #method_name_lit,
            method: #generated_handler,
        };

        fn #generated_handler<'a>(
            ctx: ::pitaya_core::context::Context,
            session: ::core::option::Option<::pitaya_core::session::Session>,
            req: &'a ::pitaya_core::protos::Request,
        ) -> ::std::pin::Pin<::std::boxed::Box<dyn ::std::future::Future<Output = ::pitaya_core::protos::Response> + Send + 'static>> {
            // calling original handler
            #body
        }
    };

    TokenStream::from(output)
}
